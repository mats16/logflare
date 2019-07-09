defmodule Logflare.Logs.Search.ParserTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Logs.Search.Parser

  describe "Parser parse" do
    test "simple message search string" do
      str = ~S|user sign up|
      {:ok, result} = Parser.parse(str)

      assert result == [
               %{operator: "~", path: "event_message", value: "user"},
               %{operator: "~", path: "event_message", value: "sign"},
               %{operator: "~", path: "event_message", value: "up"}
             ]
    end

    test "quoted message search string" do
      str = ~S|new "user sign up" server|
      {:ok, result} = Parser.parse(str)

      assert result == [
               %{operator: "~", path: "event_message", value: "user sign up"},
               %{operator: "~", path: "event_message", value: "new"},
               %{operator: "~", path: "event_message", value: "server"}
             ]
    end

    test "nested fields filter" do
      str = ~S|
        metadata.user.type:paid
        metadata.user.id:<1
        metadata.user.views:<=1
        metadata.users.source_count:>100
        metadata.context.error_count:>=100
        metadata.user.about:~referrall
      |

      {:ok, result} = Parser.parse(str)

      assert result == [
               %{operator: "=", path: "metadata.user.type", value: "paid"},
               %{operator: "<", path: "metadata.user.id", value: 1},
               %{operator: "<=", path: "metadata.user.views", value: 1},
               %{operator: ">", path: "metadata.users.source_count", value: 100},
               %{operator: ">=", path: "metadata.context.error_count", value: 100},
               %{operator: "~", path: "metadata.user.about", value: "referrall"}
             ]
    end

    test "nested fields filter 2" do
      str = ~S|
         log "was generated" "by logflare pinger"
         metadata.context.file:"some module.ex"
         metadata.context.line_number:100
         metadata.user.group_id:5
         metadata.user.admin:false
         metadata.log.label1:~origin
         metadata.log.metric1:<10
         metadata.log.metric2:<=10
         metadata.log.metric3:>10
         metadata.log.metric4:>=10
       |

      {:ok, result} = Parser.parse(str)

      assert Enum.sort(result) ==
               Enum.sort([
                 %{operator: "<", path: "metadata.log.metric1", value: 10},
                 %{operator: "<=", path: "metadata.log.metric2", value: 10},
                 %{operator: "=", path: "metadata.context.file", value: "some module.ex"},
                 %{operator: "=", path: "metadata.context.line_number", value: 100},
                 %{operator: "=", path: "metadata.user.admin", value: "false"},
                 %{operator: "=", path: "metadata.user.group_id", value: 5},
                 %{value: 10, operator: ">", path: "metadata.log.metric3"},
                 %{operator: ">=", path: "metadata.log.metric4", value: 10},
                 %{operator: "~", path: "event_message", value: "by logflare pinger"},
                 %{operator: "~", path: "event_message", value: "log"},
                 %{operator: "~", path: "event_message", value: "was generated"},
                 %{operator: "~", path: "metadata.log.label1", value: "origin"}
               ])
    end
  end
end