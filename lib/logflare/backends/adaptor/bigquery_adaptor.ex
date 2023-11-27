defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  use GenServer
  use TypedStruct

  typedstruct enforce: true do
    field(:pipeline_name, tuple())
    field(:user_id, integer())
    field(:project_id, binary())
    field(:source, atom())
    field(:ttl, integer())
    field(:dataset_location, binary())
    field(:dataset_id, binary())
  end

  @behaviour Logflare.Backends.Adaptor

  @impl Logflare.Backends.Adaptor
  def start_link(source_backend) do
    __MODULE__.Pipeline.start_link(source_backend)
  end

  @impl Logflare.Backends.Adaptor
  def ingest(pid, events) do
    Broadway.push_messages(pid, events)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_id, _query),
    do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{}}
    |> Ecto.Changeset.cast(params, [])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset),
    do: changeset

  @impl GenServer
  def init(_) do
    {:ok, []}
  end
end
