defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  @behaviour Logflare.Backends.Adaptor

  @impl Logflare.Backends.Adaptor
  def start_link(_source_backend) do
    :ignore
  end

  @impl Logflare.Backends.Adaptor
  def ingest(_id, _events) do
    raise "Unimplemented"
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
end
