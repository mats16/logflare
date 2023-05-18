defmodule Logflare.Backends.Adaptor.PostgresAdaptor.RepoTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.PostgresAdaptor.Repo
  alias Logflare.Backends.Adaptor.PostgresAdaptor.LogEvent
  alias Logflare.Backends.Adaptor.PostgresAdaptor.RepoTest.BadMigration

  import Ecto.Query

  setup do
    %{username: username, password: password, database: database, hostname: hostname} =
      Application.get_env(:logflare, Logflare.Repo) |> Map.new()

    url = "postgresql://#{username}:#{password}@#{hostname}/#{database}"
    source = insert(:source, user: insert(:user))
    source_backend = insert(:source_backend, type: :postgres, config: %{url: url}, source: source)

    %{source_backend: source_backend}
  end

  describe "new_repository_for_source_backend/1" do
    test "creates a new Ecto.Repo for given source_backend", %{source_backend: source_backend} do
      repository_module = Repo.new_repository_for_source_backend(source_backend)
      assert Keyword.get(repository_module.__info__(:attributes), :behaviour) == [Ecto.Repo]
    end

    test "name of the module uses source_id", %{source_backend: source_backend} do
      repository_module = Repo.new_repository_for_source_backend(source_backend)

      assert repository_module ==
               Module.concat([Logflare.Repo.Postgres, "Adaptor#{source_backend.source.token}"])
    end
  end

  describe "create_log_event_table/1" do
    setup %{source_backend: source_backend} do
      repository_module = Repo.new_repository_for_source_backend(source_backend)

      Repo.connect_to_source_backend(
        repository_module,
        source_backend,
        pool: Ecto.Adapters.SQL.Sandbox
      )

      Ecto.Adapters.SQL.Sandbox.mode(repository_module, :auto)

      on_exit(fn ->
        Ecto.Migrator.run(repository_module, Repo.migrations(source_backend), :down, all: true)
        migration_table = Keyword.get(repository_module.config(), :migration_source)
        Ecto.Adapters.SQL.query!(repository_module, "DROP TABLE IF EXISTS #{migration_table}")
      end)

      %{repository_module: repository_module}
    end

    test "runs migration for the newly created connection", %{
      source_backend: source_backend,
      repository_module: repository_module
    } do
      assert Repo.create_log_event_table(repository_module, source_backend) == :ok

      query = from(l in Repo.table_name(source_backend), select: LogEvent)
      assert repository_module.all(query) == []
    end

    test "handle migration errors", %{
      source_backend: source_backend,
      repository_module: repository_module
    } do
      bad_migrations = [{0, BadMigration}]

      assert Repo.create_log_event_table(
               repository_module,
               source_backend,
               bad_migrations
             ) == {:error, :failed_migration}
    end
  end

  defmodule BadMigration do
    use Ecto.Migration

    def up do
      alter table(:none) do
      end
    end
  end
end