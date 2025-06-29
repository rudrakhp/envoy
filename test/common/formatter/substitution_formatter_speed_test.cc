#include "source/common/formatter/substitution_format_utility.h"
#include "source/common/formatter/substitution_formatter.h"
#include "source/common/network/address_impl.h"

#include "test/common/stream_info/test_util.h"
#include "test/mocks/http/mocks.h"

#include "benchmark/benchmark.h"

namespace Envoy {

namespace {

std::unique_ptr<Envoy::Formatter::JsonFormatterImpl> makeJsonFormatter() {
  ProtobufWkt::Struct struct_format;
  const std::string format_yaml = R"EOF(
    remote_address: '%DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT%'
    start_time: '%START_TIME(%Y/%m/%dT%H:%M:%S%z %s)%'
    method: '%REQ(:METHOD)%'
    url: '%REQ(X-FORWARDED-PROTO)%://%REQ(:AUTHORITY)%%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%'
    protocol: '%PROTOCOL%'
    response_code: '%RESPONSE_CODE%'
    bytes_sent: '%BYTES_SENT%'
    duration: '%DURATION%'
    referer: '%REQ(REFERER)%'
    user-agent: '%REQ(USER-AGENT)%'
  )EOF";
  TestUtility::loadFromYaml(format_yaml, struct_format);
  return std::make_unique<Envoy::Formatter::JsonFormatterImpl>(struct_format, false);
}

std::unique_ptr<Envoy::TestStreamInfo> makeStreamInfo(TimeSource& time_source) {
  auto stream_info = std::make_unique<Envoy::TestStreamInfo>(time_source);
  stream_info->downstream_connection_info_provider_->setRemoteAddress(
      std::make_shared<Envoy::Network::Address::Ipv4Instance>("203.0.113.1"));
  return stream_info;
}

} // namespace

// Test measures how fast Formatters are constructed from
// LogFormat string.
// NOLINTNEXTLINE(readability-identifier-naming)
static void BM_AccessLogFormatterSetup(benchmark::State& state) {
  static const char* LogFormat =
      "%DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT% %START_TIME(%Y/%m/%dT%H:%M:%S%z %s)% "
      "%REQ(:METHOD)% "
      "%REQ(X-FORWARDED-PROTO)%://%REQ(:AUTHORITY)%%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)% %PROTOCOL% "
      "s%RESPONSE_CODE% %BYTES_SENT% %DURATION% %REQ(REFERER)% \"%REQ(USER-AGENT)%\" - - -\n";

  for (auto _ : state) { // NOLINT: Silences warning about dead store
    std::unique_ptr<Envoy::Formatter::FormatterImpl> formatter =
        *Envoy::Formatter::FormatterImpl::create(LogFormat, false);
  }
}
BENCHMARK(BM_AccessLogFormatterSetup);

// NOLINTNEXTLINE(readability-identifier-naming)
static void BM_AccessLogFormatter(benchmark::State& state) {
  testing::NiceMock<MockTimeSystem> time_system;

  std::unique_ptr<Envoy::TestStreamInfo> stream_info = makeStreamInfo(time_system);
  static const char* LogFormat =
      "%DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT% %START_TIME(%Y/%m/%dT%H:%M:%S%z %s)% "
      "%REQ(:METHOD)% "
      "%REQ(X-FORWARDED-PROTO)%://%REQ(:AUTHORITY)%%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)% %PROTOCOL% "
      "s%RESPONSE_CODE% %BYTES_SENT% %DURATION% %REQ(REFERER)% \"%REQ(USER-AGENT)%\" - - -\n";

  std::unique_ptr<Envoy::Formatter::FormatterImpl> formatter =
      *Envoy::Formatter::FormatterImpl::create(LogFormat, false);

  size_t output_bytes = 0;
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    output_bytes += formatter->formatWithContext({}, *stream_info).length();
  }
  benchmark::DoNotOptimize(output_bytes);
}
BENCHMARK(BM_AccessLogFormatter);

// NOLINTNEXTLINE(readability-identifier-naming)
static void BM_AccessLogFormatterTextMockJson(benchmark::State& state) {
  testing::NiceMock<MockTimeSystem> time_system;

  std::unique_ptr<Envoy::TestStreamInfo> stream_info = makeStreamInfo(time_system);
  ProtobufWkt::Struct struct_format;
  const std::string format_yaml = R"EOF(
    remote_address: '%DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT%'
    start_time: '%START_TIME(%Y/%m/%dT%H:%M:%S%z %s)%'
    method: '%REQ(:METHOD)%'
    url: '%REQ(X-FORWARDED-PROTO)%://%REQ(:AUTHORITY)%%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%'
    protocol: '%PROTOCOL%'
    response_code: '%RESPONSE_CODE%'
    bytes_sent: '%BYTES_SENT%'
    duration: '%DURATION%'
    referer: '%REQ(REFERER)%'
    user-agent: '%REQ(USER-AGENT)%'
  )EOF";
  TestUtility::loadFromYaml(format_yaml, struct_format);

  const std::string LogFormat = MessageUtil::getJsonStringFromMessageOrError(struct_format);

  std::unique_ptr<Envoy::Formatter::FormatterImpl> formatter =
      *Envoy::Formatter::FormatterImpl::create(LogFormat, false);

  size_t output_bytes = 0;
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    output_bytes += formatter->formatWithContext({}, *stream_info).length();
  }
  benchmark::DoNotOptimize(output_bytes);
}
BENCHMARK(BM_AccessLogFormatterTextMockJson);

// NOLINTNEXTLINE(readability-identifier-naming)
static void BM_JsonAccessLogFormatter(benchmark::State& state) {
  testing::NiceMock<MockTimeSystem> time_system;

  std::unique_ptr<Envoy::TestStreamInfo> stream_info = makeStreamInfo(time_system);
  std::unique_ptr<Envoy::Formatter::JsonFormatterImpl> json_formatter = makeJsonFormatter();

  size_t output_bytes = 0;
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    output_bytes += json_formatter->formatWithContext({}, *stream_info).length();
  }
  benchmark::DoNotOptimize(output_bytes);
}
BENCHMARK(BM_JsonAccessLogFormatter);

// NOLINTNEXTLINE(readability-identifier-naming)
static void BM_FormatterCommandParsing(benchmark::State& state) {
  const std::string token = "Listener:namespace:key";
  absl::string_view listener, names, key;
  for (auto _ : state) { // NOLINT: Silences warning about dead store
    Formatter::SubstitutionFormatUtils::parseSubcommand(token, ':', listener, names, key);
  }
}
BENCHMARK(BM_FormatterCommandParsing);
} // namespace Envoy
