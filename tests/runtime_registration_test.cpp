#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <catch2/catch_test_macros.hpp>

#include <tesseron/host.hpp>

#include "gateway_double.hpp"

using tesseron::Action;
using tesseron::ActionContext;
using tesseron::ActionDefinition;
using tesseron::Host;
using tesseron::HostEvent;
using tesseron::HostOptions;
using tesseron::Json;
using tesseron::ResourceDefinition;
using tesseron::ResourceEmitter;
using tesseron::Result;
using tesseron::Subscription;
using tesseron::testing::GatewayDouble;

namespace {

constexpr std::chrono::milliseconds kSilence{100};

Action numbered_action(std::string name, int value) {
  return ActionDefinition(std::move(name)).handler(
      [value](Json, ActionContext) -> boost::asio::awaitable<Result<Json>> {
        co_return Json(value);
      });
}

boost::asio::awaitable<Result<Json>> read_number() { co_return Json(7); }

Json request(std::string id, std::string method, Json params) {
  return {{"jsonrpc", "2.0"}, {"id", std::move(id)}, {"method", std::move(method)},
          {"params", std::move(params)}};
}

Json next_frame(GatewayDouble& gateway) {
  auto frame = gateway.receive();
  REQUIRE(frame.has_value());
  return std::move(*frame);
}

Json changed_list(GatewayDouble& gateway, const std::string& kind) {
  const auto frame = next_frame(gateway);
  REQUIRE(frame.at("method") == kind + "/list_changed");
  REQUIRE_FALSE(frame.contains("id"));
  REQUIRE(frame.at("params").size() == 1);
  return frame.at("params").at(kind);
}

struct RuntimeHost {
  std::mutex guard;
  std::condition_variable arrival;
  unsigned welcomes = 0;
  std::optional<Host> host;

  RuntimeHost() {
    HostOptions options;
    options.manifest = tesseron::ManifestPublication::disabled();
    auto builder = Host::builder();
    builder.application("runtime", "Runtime").options(std::move(options));
    builder.on_event([this](const HostEvent& event) {
      if (event.kind != HostEvent::Kind::Welcome) return;
      {
        const std::lock_guard<std::mutex> holding(guard);
        ++welcomes;
      }
      arrival.notify_all();
    });
    builder.action("first").handler(
        [](Json, ActionContext) -> boost::asio::awaitable<Result<Json>> { co_return Json(1); });
    builder.resource("plain").reader(read_number);
    auto listening = builder.listen();
    REQUIRE(listening.ok());
    host = std::move(listening).value();
  }

  void accept(GatewayDouble& gateway, const Json& hello) {
    unsigned expected;
    {
      const std::lock_guard<std::mutex> holding(guard);
      expected = welcomes + 1;
    }
    gateway.accept_handshake(hello);
    std::unique_lock<std::mutex> waiting(guard);
    REQUIRE(arrival.wait_for(waiting, tesseron::testing::kPatience,
                             [this, expected] { return welcomes == expected; }));
  }

  Json open(GatewayDouble& gateway) {
    auto hello = next_frame(gateway);
    REQUIRE(hello.at("method") == "tesseron/hello");
    accept(gateway, hello);
    return hello;
  }
};

struct SubscriberRecord {
  std::mutex guard;
  std::optional<ResourceEmitter> emitter;
  std::atomic<int> teardowns{0};

  void remember(ResourceEmitter handed) {
    const std::lock_guard<std::mutex> holding(guard);
    emitter = std::move(handed);
    emitter->emit(Json(7));
  }

  void emit() {
    const std::lock_guard<std::mutex> holding(guard);
    REQUIRE(emitter.has_value());
    emitter->emit(Json(8));
  }
};

void subscribe(GatewayDouble& gateway, const std::string& name, const std::string& id) {
  gateway.send(request(id, "resources/subscribe", {{"name", name}, {"subscriptionId", id}}));
  REQUIRE(next_frame(gateway).at("id") == id);
  const auto update = next_frame(gateway);
  REQUIRE(update.at("method") == "resources/updated");
  REQUIRE(update.at("params").at("subscriptionId") == id);
}

}  // namespace

TEST_CASE("register_action after welcome sends actions/list_changed with the full list and the next hello lists it",
          "[runtime-registration]") {
  RuntimeHost probe;
  Json expected;
  {
    GatewayDouble gateway(probe.host->url());
    const auto hello = probe.open(gateway);
    auto action = ActionDefinition("second")
                      .description("A validated action")
                      .input(tesseron::schema::object({
                          tesseron::schema::required("number", tesseron::schema::number()),
                      }))
                      .output_schema({{"type", "number"}})
                      .timeout(std::chrono::milliseconds(2000))
                      .handler([](Json input, ActionContext) -> boost::asio::awaitable<Result<Json>> {
                        co_return input.at("number");
                      });
    expected = hello.at("params").at("actions");
    expected.push_back(action.descriptor.to_json());
    probe.host->register_action(std::move(action));
    REQUIRE(changed_list(gateway, "actions") == expected);
    gateway.send(request("invalid", "actions/invoke",
                         {{"name", "second"}, {"invocationId", "invalid"}, {"input", Json::object()}}));
    REQUIRE(next_frame(gateway).at("error").at("code") == -32004);
    gateway.send(request("valid", "actions/invoke",
                         {{"name", "second"}, {"invocationId", "valid"}, {"input", {{"number", 9}}}}));
    REQUIRE(next_frame(gateway).at("result").at("output") == 9);
  }
  GatewayDouble reconnected(probe.host->url());
  const auto resume = next_frame(reconnected);
  REQUIRE(resume.at("method") == "tesseron/resume");
  REQUIRE(resume.at("params").at("actions") == expected);
  reconnected.send({{"jsonrpc", "2.0"}, {"id", resume.at("id")},
                    {"error", {{"code", -32001}, {"message", "session expired"}}}});
  const auto hello = next_frame(reconnected);
  REQUIRE(hello.at("method") == "tesseron/hello");
  REQUIRE(hello.at("params").at("actions") == expected);
  probe.accept(reconnected, hello);
}

TEST_CASE("remove_action returns false for an unknown name and sends nothing; true for a known one and sends the shrunken list",
          "[runtime-registration]") {
  RuntimeHost probe;
  GatewayDouble gateway(probe.host->url());
  probe.open(gateway);
  REQUIRE_FALSE(probe.host->remove_action("missing"));
  REQUIRE_FALSE(gateway.receive(kSilence).has_value());
  REQUIRE(probe.host->remove_action("first"));
  REQUIRE(changed_list(gateway, "actions") == Json::array());
  REQUIRE_FALSE(probe.host->remove_action("first"));
  gateway.send(request("gone", "actions/invoke", {{"name", "first"}, {"invocationId", "gone"}}));
  REQUIRE(next_frame(gateway).at("error").at("code") == -32003);

  probe.host->register_action(ActionDefinition("self_removing").handler(
      [&probe](Json, ActionContext) -> boost::asio::awaitable<Result<Json>> {
        const bool removed = probe.host->remove_action("self_removing");
        co_return Json(removed);
      }));
  REQUIRE(changed_list(gateway, "actions").at(0).at("name") == "self_removing");
  gateway.send(request("running", "actions/invoke",
                       {{"name", "self_removing"}, {"invocationId", "running"}}));
  auto first = next_frame(gateway);
  auto second = next_frame(gateway);
  if (first.contains("method")) std::swap(first, second);
  REQUIRE(first.at("result").at("output") == true);
  REQUIRE(second.at("method") == "actions/list_changed");
  REQUIRE(second.at("params").at("actions") == Json::array());
}

TEST_CASE("register_resource and remove_resource notify; remove_resource drops the live subscription and its teardown runs exactly once",
          "[runtime-registration]") {
  RuntimeHost probe;
  auto watched = std::make_shared<SubscriberRecord>();
  auto retained = std::make_shared<SubscriberRecord>();
  bool throws = false;
  SECTION("normal teardown") {}
  SECTION("throwing teardown does not stop the other subscriptions") { throws = true; }
  GatewayDouble gateway(probe.host->url());
  const auto hello = probe.open(gateway);
  auto resource = ResourceDefinition("counter").description("Counter")
      .subscribe([watched, throws](ResourceEmitter emitter) {
        watched->remember(std::move(emitter));
        return Subscription::with_teardown([watched, throws] {
          watched->teardowns.fetch_add(1);
          if (throws) throw std::runtime_error("teardown failure");
        });
      }).reader(read_number);
  auto expected = hello.at("params").at("resources");
  expected.push_back(resource.descriptor.to_json());
  probe.host->register_resource(std::move(resource));
  REQUIRE(changed_list(gateway, "resources") == expected);
  gateway.send(request("read-counter", "resources/read", {{"name", "counter"}}));
  REQUIRE(next_frame(gateway).at("result").at("value") == 7);
  probe.host->register_resource(ResourceDefinition("plain")
      .subscribe([retained](ResourceEmitter emitter) {
        retained->remember(std::move(emitter));
        return Subscription::with_teardown([retained] { retained->teardowns.fetch_add(1); });
      }).reader(read_number));
  auto replacement = changed_list(gateway, "resources");
  REQUIRE(replacement.size() == 2);
  REQUIRE(replacement.at(0).at("name") == "plain");
  REQUIRE(replacement.at(1) == expected.at(1));
  subscribe(gateway, "counter", "counter-1");
  subscribe(gateway, "counter", "counter-2");
  subscribe(gateway, "plain", "plain-1");
  REQUIRE_FALSE(probe.host->remove_resource("missing"));
  REQUIRE_FALSE(gateway.receive(kSilence).has_value());
  REQUIRE(probe.host->remove_resource("counter"));
  replacement.erase(1);
  REQUIRE(changed_list(gateway, "resources") == replacement);
  REQUIRE(watched->teardowns.load() == 2);
  REQUIRE(retained->teardowns.load() == 0);
  watched->emit();
  REQUIRE_FALSE(gateway.receive(kSilence).has_value());
  retained->emit();
  REQUIRE(next_frame(gateway).at("params").at("subscriptionId") == "plain-1");
  gateway.send(request("unsubscribed", "resources/unsubscribe", {{"subscriptionId", "counter-1"}}));
  REQUIRE(next_frame(gateway).at("id") == "unsubscribed");
  gateway.send(request("removed", "resources/read", {{"name", "counter"}}));
  REQUIRE(next_frame(gateway).at("error").at("code") == -32003);
  REQUIRE(probe.host->shutdown().ok());
  REQUIRE(watched->teardowns.load() == 2);
  REQUIRE(retained->teardowns.load() == 1);
}

TEST_CASE("register_action with an existing name replaces it in place and notifies once",
          "[runtime-registration]") {
  RuntimeHost probe;
  probe.host->register_action(numbered_action("second", 2));
  GatewayDouble gateway(probe.host->url());
  const auto hello = probe.open(gateway);
  auto replacement = ActionDefinition("first").description("Replacement")
      .input_schema({{"type", "object"}}, [](const Json&) {
        return std::vector<tesseron::ValidationIssue>{};
      }).handler([](Json, ActionContext) -> boost::asio::awaitable<Result<Json>> { co_return Json(3); });
  auto expected = hello.at("params").at("actions");
  expected.at(0) = replacement.descriptor.to_json();
  probe.host->register_action(std::move(replacement));
  REQUIRE(changed_list(gateway, "actions") == expected);
  REQUIRE_FALSE(gateway.receive(kSilence).has_value());
  gateway.send(request("replacement", "actions/invoke",
                       {{"name", "first"}, {"invocationId", "replacement"}, {"input", Json::object()}}));
  REQUIRE(next_frame(gateway).at("result").at("output") == 3);
}

TEST_CASE("register_action before welcome sends nothing, and the first hello includes it",
          "[runtime-registration]") {
  RuntimeHost probe;
  auto action = numbered_action("before", 2);
  const auto expected = action.descriptor.to_json();
  probe.host->register_action(std::move(action));
  GatewayDouble gateway(probe.host->url());
  const auto hello = next_frame(gateway);
  REQUIRE(hello.at("method") == "tesseron/hello");
  REQUIRE(hello.at("params").at("actions").size() == 2);
  REQUIRE(hello.at("params").at("actions").at(1) == expected);
  REQUIRE_FALSE(gateway.receive(kSilence).has_value());
  probe.accept(gateway, hello);
  REQUIRE_FALSE(gateway.receive(kSilence).has_value());
}

TEST_CASE("a mutation between hello and welcome is announced right after welcome",
          "[runtime-registration]") {
  RuntimeHost probe;
  GatewayDouble gateway(probe.host->url());
  const auto hello = next_frame(gateway);
  REQUIRE(hello.at("method") == "tesseron/hello");
  auto action = numbered_action("pending", 2);
  auto resource = ResourceDefinition("pending").reader(read_number);
  auto actions = hello.at("params").at("actions");
  auto resources = hello.at("params").at("resources");
  actions.push_back(action.descriptor.to_json());
  resources.push_back(resource.descriptor.to_json());
  probe.host->register_action(std::move(action));
  probe.host->register_resource(std::move(resource));
  auto latest = numbered_action("latest", 3);
  actions.push_back(latest.descriptor.to_json());
  probe.host->register_action(std::move(latest));
  REQUIRE(probe.host->remove_action("first"));
  actions.erase(0);
  REQUIRE_FALSE(gateway.receive(kSilence).has_value());

  SECTION("accepted welcome flushes both current lists") {
    probe.accept(gateway, hello);
    REQUIRE(changed_list(gateway, "actions") == actions);
    REQUIRE(changed_list(gateway, "resources") == resources);
    REQUIRE_FALSE(gateway.receive(kSilence).has_value());
  }
  SECTION("failed handshake discards pending announcements") {
    gateway.accept_handshake(hello, "99.0.0");
    REQUIRE(gateway.closed());
    REQUIRE_FALSE(gateway.receive(kSilence).has_value());
    probe.host->register_action(numbered_action("after_failure", 3));
    REQUIRE_FALSE(gateway.receive(kSilence).has_value());
  }
  SECTION("closed transport discards pending announcements") {
    REQUIRE(probe.host->shutdown().ok());
    REQUIRE(gateway.closed());
    REQUIRE_FALSE(gateway.receive(kSilence).has_value());
  }
}
