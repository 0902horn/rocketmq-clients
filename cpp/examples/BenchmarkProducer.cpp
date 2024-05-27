/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <random>
#include <string>
#include <system_error>

#include "rocketmq/FifoProducer.h"
#include "rocketmq/Message.h"
#include "rocketmq/Producer.h"

using namespace ROCKETMQ_NAMESPACE;

const std::string& alphaNumeric() {
  static std::string alpha_numeric("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
  return alpha_numeric;
}

std::string randomString(std::string::size_type len) {
  std::string result;
  result.reserve(len);
  std::random_device rd;
  std::mt19937 generator(rd());
  std::string source(alphaNumeric());
  std::string::size_type generated = 0;
  while (generated < len) {
    std::shuffle(source.begin(), source.end(), generator);
    std::string::size_type delta = std::min({len - generated, source.length()});
    result.append(source.substr(0, delta));
    generated += delta;
  }
  return result;
}

const std::string access_point = "121.196.167.124:8081";
const std::int32_t message_body_size = 4096;
const std::uint32_t total = 1000;
const std::string access_key = "";
const std::string access_secret = "";
const std::string fifo_topic = "fifo_topic";
const std::string non_fifo_topic = "non_fifo_topic";
const bool use_tls = false;

class ProducerFixture : public benchmark::Fixture {
public:
  ProducerFixture() {
  }

  void SetUp(::benchmark::State& state) override {
    std::cout << "Set up ProducerFixture" << std::endl;
    auto cred_provider = std::make_shared<StaticCredentialsProvider>(access_key, access_secret);
    body_ = randomString(message_body_size);
    producer_ = std::make_shared<Producer>(Producer::newBuilder()
                                               .withConfiguration(Configuration::newBuilder()
                                                                      .withEndpoints(access_point)
                                                                      .withCredentialsProvider(cred_provider)
                                                                      .withSsl(use_tls)
                                                                      .build())
                                               .withTopics({fifo_topic, non_fifo_topic})
                                               .build());

    fifo_producer_ = std::make_shared<FifoProducer>(FifoProducer::newBuilder()
                                                        .withConfiguration(Configuration::newBuilder()
                                                                               .withEndpoints(access_point)
                                                                               .withCredentialsProvider(cred_provider)
                                                                               .withSsl(use_tls)
                                                                               .build())
                                                        .withConcurrency(10)
                                                        .withTopics({fifo_topic})
                                                        .build());
  }

  void TearDown(::benchmark::State& state) override {
    std::cout << "Tear down ProducerFixture" << std::endl;
    producer_.reset();
  }

  std::shared_ptr<Producer> producer_;
  std::shared_ptr<FifoProducer> fifo_producer_;
  std::string body_;
};

BENCHMARK_DEFINE_F(ProducerFixture, SendFifoMessages)(benchmark::State& st) {
  for (auto _ : st) {
    for (int64_t i = 0; i < st.range(0); i++) {
      auto message = Message::newBuilder()
                         .withTopic(fifo_topic)
                         .withTag("TagA")
                         .withKeys({"Key-0"})
                         .withBody(body_)
                         .withGroup("message-group" + std::to_string(i % 10))
                         .build();
      std::error_code ec;
      SendReceipt send_receipt = producer_->send(std::move(message), ec);
      if (ec) {
        std::cerr << "Failed to publish message to " << fifo_topic << ". Cause: " << ec.message() << std::endl;
      }
    }
  }
}

BENCHMARK_DEFINE_F(ProducerFixture, SendFifoMessages2)(benchmark::State& st) {
  for (auto _ : st) {
    for (int64_t i = 0; i < st.range(0); i++) {
      auto message = Message::newBuilder()
                         .withTopic(fifo_topic)
                         .withTag("TagB")
                         .withKeys({"Key-0"})
                         .withBody(body_)
                         .withGroup("message-group" + std::to_string(i % 10))
                         .build();
      auto callback = [](const std::error_code& ec, const SendReceipt& receipt) mutable {
        if (ec) {
          std::cerr << "Failed to publish message to " << non_fifo_topic << ". Cause: " << ec.message() << std::endl;
        }
      };
      fifo_producer_->send(std::move(message), callback);
    }
  }
}

BENCHMARK_DEFINE_F(ProducerFixture, SendNonFifoMessages)(benchmark::State& st) {
  for (auto _ : st) {
    for (int64_t i = 0; i < st.range(0); i++) {
      auto message = Message::newBuilder()
                         .withTopic(non_fifo_topic)
                         .withTag("TagA")
                         .withKeys({"Key-" + std::to_string(i)})
                         .withBody(body_)
                         .build();
      std::error_code ec;
      SendReceipt send_receipt = producer_->send(std::move(message), ec);
      if (ec) {
        std::cerr << "Failed to publish message to " << non_fifo_topic << ". Cause: " << ec.message() << std::endl;
      }
    }
  }
}

BENCHMARK_REGISTER_F(ProducerFixture, SendFifoMessages)->Arg(total);
BENCHMARK_REGISTER_F(ProducerFixture, SendFifoMessages2)->Arg(total);
BENCHMARK_REGISTER_F(ProducerFixture, SendNonFifoMessages)->Arg(total);

// See https://github.com/google/benchmark for usage.
BENCHMARK_MAIN();
