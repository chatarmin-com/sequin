defmodule Sequin.Sinks.Redis.ClientTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error.ServiceError
  alias Sequin.Error.TimeoutError
  alias Sequin.Runtime.Routing.RoutedMessage
  alias Sequin.Sinks.Redis.Client
  alias Sequin.Sinks.Redis.ConnectionCache

  @moduletag capture_log: true

  defmodule PipelineConnection do
    @moduledoc false
    use GenServer

    @impl GenServer
    def init(state), do: {:ok, state}

    @impl GenServer
    def handle_call({:pipeline, commands}, _from, {owner, [result | remaining]}) do
      send(owner, {:pipeline, Enum.map(commands, &IO.iodata_to_binary/1)})
      {:reply, result, {owner, remaining}}
    end

    @impl GenServer
    def handle_call(:stop, _from, state), do: {:stop, :normal, :ok, state}
  end

  test "successful stream pipelines return success" do
    consumer = consumer_with_results([[{:ok, "1-0"}, {:ok, "2-0"}]])

    assert :ok =
             Client.send_messages(consumer, [stream_message("event-1"), stream_message("event-2")])
  end

  for position <- 0..2 do
    test "a rejected stream command at position #{position} fails the batch" do
      results =
        List.replace_at(
          [{:ok, "1-0"}, {:ok, "2-0"}, {:ok, "3-0"}],
          unquote(position),
          {:error, "WRONGTYPE"}
        )

      consumer = consumer_with_results([results])
      messages = Enum.map(1..3, &stream_message("event-#{&1}"))

      assert {:error, %ServiceError{code: :command_failed, message: message}} =
               Client.send_messages(consumer, messages)

      assert message =~ "WRONGTYPE"
    end
  end

  test "retrying a partially failed batch preserves its commands and event keys" do
    consumer =
      consumer_with_results([[{:ok, "1-0"}, {:error, "NOPERM"}], [{:ok, "2-0"}, {:ok, "3-0"}]])

    messages = [stream_message("event-1"), stream_message("event-2")]

    assert {:error, %ServiceError{code: :command_failed}} =
             Client.send_messages(consumer, messages)

    assert_receive {:pipeline, original_commands}

    assert :ok = Client.send_messages(consumer, messages)
    assert_receive {:pipeline, retried_commands}
    assert retried_commands == original_commands
  end

  test "top-level connection failures still return delivery errors" do
    consumer = consumer_with_results([{:error, :no_connection}])

    assert {:error, %ServiceError{code: :no_connection}} =
             Client.send_messages(consumer, [stream_message("event-1")])
  end

  test "top-level timeouts still return delivery errors" do
    consumer = consumer_with_results([{:error, :timeout}])

    assert {:error, %TimeoutError{}} = Client.send_messages(consumer, [stream_message("event-1")])
  end

  test "successful Redis String pipelines return success" do
    sink = string_sink_with_results([[{:ok, "OK"}, {:ok, "1"}]])
    messages = [string_message(), %RoutedMessage{routing_info: %{action: "del", key: "old-key"}}]

    assert :ok = Client.set_messages(sink, messages)
  end

  test "a rejected Redis String command fails the batch" do
    sink = string_sink_with_results([[{:ok, "OK"}, {:error, "NOPERM"}]])
    messages = [string_message(), %RoutedMessage{routing_info: %{action: "del", key: "old-key"}}]

    assert {:error, %ServiceError{code: :command_failed, message: message}} =
             Client.set_messages(sink, messages)

    assert message =~ "NOPERM"
  end

  defp consumer_with_results(results) do
    sink = %RedisStreamSink{connection_id: connection_id(), stream_key: "test-stream"}
    cache_connection(sink, results)
    %SinkConsumer{sink: sink}
  end

  defp string_sink_with_results(results) do
    sink = %RedisStringSink{connection_id: connection_id()}
    cache_connection(sink, results)
    sink
  end

  defp cache_connection(sink, results) do
    # Exercise the actual eredis pipeline API while controlling the connection's replies.
    {:ok, connection} = GenServer.start(PipelineConnection, {self(), results})
    :ok = ConnectionCache.cache_connection(sink, connection)

    on_exit(fn ->
      ConnectionCache.invalidate_connection(sink)
      ConnectionCache.existing_connection(ConnectionCache, sink)
    end)
  end

  defp connection_id, do: Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)

  defp stream_message(event_key) do
    %RoutedMessage{
      routing_info: %{stream_key: "test-stream"},
      transformed_message: %{"idempotency_key" => event_key}
    }
  end

  defp string_message do
    %RoutedMessage{
      routing_info: %{action: "set", key: "test-key", expire_ms: nil},
      transformed_message: "value"
    }
  end
end
