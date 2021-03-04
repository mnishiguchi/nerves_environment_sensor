defmodule NervesEnvironmentSensor.Worker do
  @moduledoc """
  Repeats posting a sensor measurement to an external API.
  """

  use GenServer, restart: :temporary

  require Logger

  @default_interval :timer.seconds(1)

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  def whereis, do: Process.whereis(__MODULE__)
  def get_state, do: :sys.get_state(__MODULE__)
  def shutdown, do: Process.exit(whereis(), :shutdown)

  @impl true
  def init(opts \\ []) do
    {:ok, sensor_pid} = init_sensor(bus_name: opts[:bus_name], bus_address: opts[:bus_address])

    # Interval and measurement do not have to be kept in the state but are nice to have for ease of testing.
    state = %{
      interval: opts[:interval] || @default_interval,
      measurement: nil,
      sensor_pid: sensor_pid
    }

    {:ok, state, {:continue, :after_init}}
  end

  @impl true
  def handle_continue(:after_init, state) do
    send(self(), :schedule_measurement)
    {:noreply, state}
  end

  @impl true
  def handle_info(:schedule_measurement, %{sensor_pid: sensor_pid, interval: interval} = state) do
    Process.send_after(self(), :schedule_measurement, interval)

    case read_sensor(sensor_pid) do
      {:error, reason} ->
        Logger.error("Error reading sensor: #{reason}")
        {:noreply, %{state | measurement: %{error: reason}}}

      {:ok, new_measurement} ->
        Logger.info("measurement: #{inspect(new_measurement)}")

        case post_measurement(new_measurement) do
          {:ok, %{status_code: 201}} ->
            Logger.info("Success posting measurement")
            {:noreply, %{state | measurement: new_measurement}}

          {:ok, %{status_code: status_code}} ->
            reason = Plug.Conn.Status.reason_atom(status_code)
            Logger.error("Error posting measurement: #{reason}")
            {:noreply, %{state | measurement: %{error: reason}}}

          {:error, %{reason: reason}} ->
            Logger.error("Error posting measurement: #{reason}")
            {:noreply, %{state | measurement: %{error: reason}}}
        end
    end
  end

  defp init_sensor(opts), do: apply(sensor_device_module(), :start_link, [opts])
  defp read_sensor(sensor_pid), do: apply(sensor_device_module(), :measure, [sensor_pid])
  defp post_measurement(measurement), do: apply(sensor_api_module(), :post_measurement, [measurement])

  # https://hexdocs.pm/elixir/master/library-guidelines.html#avoid-compile-time-application-configuration
  defp sensor_device_module(), do: Application.fetch_env!(:nerves_environment_sensor, :sensor_device_module)
  defp sensor_api_module(), do: Application.fetch_env!(:nerves_environment_sensor, :sensor_api_module)
end
