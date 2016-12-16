/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include <string>
#include <vector>

#include "src/common.h"
#include "src/connection.h"
#include "src/topic.h"

namespace NodeKafka {

/**
 * @brief Producer v8 wrapped object.
 *
 * Wraps the RdKafka::Producer object with compositional inheritence and
 * provides methods for interacting with it exposed to node.
 *
 * The base wrappable RdKafka::Handle deals with most of the wrapping but
 * we still need to declare its prototype.
 *
 * @sa RdKafka::Producer
 * @sa NodeKafka::Connection
 */

Connection::Topic::Topic(std::string topic_name, RdKafka::Conf* config,
    Connection * handle) :
    m_topic(NULL),
    m_topic_name(topic_name),
    m_config(config),
    m_handle(handle) {

  Baton b = handle->CreateTopic(topic_name, config);

  if (b.err() == RdKafka::ERR_NO_ERROR) {
    m_topic = b.data<RdKafka::Topic*>();
  }

}

Connection::Topic::~Topic() {
  if (m_topic) {
    delete m_topic;
  }
}

Nan::Persistent<v8::Function> Connection::Topic::constructor;

void Connection::Topic::Init(v8::Local<v8::Object> exports) {
  Nan::HandleScope scope;

  v8::Local<v8::FunctionTemplate> tpl = Nan::New<v8::FunctionTemplate>(New);
  tpl->SetClassName(Nan::New("Topic").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  Nan::SetPrototypeMethod(tpl, "name", NodeGetName);
  Nan::SetPrototypeMethod(tpl, "get", NodeGet);

  // connect. disconnect. resume. pause. get meta data
  constructor.Reset(Nan::GetFunction(tpl).ToLocalChecked());
}

void Connection::Topic::New(const Nan::FunctionCallbackInfo<v8::Value>& info) {
  if (!info.IsConstructCall()) {
    return Nan::ThrowError("non-constructor invocation not supported");
  }

  if (info.Length() < 3) {
    return Nan::ThrowError("Handle, topic name and configuration required");
  }

  if (!info[0]->IsString()) {
    return Nan::ThrowError("Topic name must be a string");
  }

  Nan::Utf8String parameterValue(info[0]->ToString());
  std::string topic_name(*parameterValue);

  if (!info[1]->IsObject()) {
    return Nan::ThrowError("Configuration data must be specified");
  }

  std::string errstr;

  RdKafka::Conf* config =
    Conf::create(RdKafka::Conf::CONF_TOPIC, info[1]->ToObject(), errstr);

  if (!config) {
    return Nan::ThrowError(errstr.c_str());
  }

  Connection* connection = ObjectWrap::Unwrap<Connection>(info[2]->ToObject());

  Topic* topic = new Topic(topic_name, config, connection);

  // Wrap it
  topic->Wrap(info.This());

  // Then there is some weird initialization that happens
  // basically it sets the configuration data
  // we don't need to do that because we lazy load it

  info.GetReturnValue().Set(info.This());
}

// handle

v8::Local<v8::Object> Connection::Topic::NewInstance(v8::Local<v8::Value> arg) {
  Nan::EscapableHandleScope scope;

  const unsigned argc = 1;

  v8::Local<v8::Value> argv[argc] = { arg };
  v8::Local<v8::Function> cons = Nan::New<v8::Function>(constructor);
  v8::Local<v8::Object> instance =
    Nan::NewInstance(cons, argc, argv).ToLocalChecked();

  return scope.Escape(instance);
}

Baton Connection::Topic::toRdKafkaTopic() {
  if (!m_handle || !m_handle->IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  return Baton(m_topic);
}

std::string Connection::Topic::Name() {
  return m_topic_name;
}

/*

bool partition_available(int32_t partition) {
  return topic_->partition_available(partition);
}

*/

NAN_METHOD(Connection::Topic::NodeGet) {
  Topic* topic = ObjectWrap::Unwrap<Topic>(info.This());

  if (info.Length() < 1) {
    return Nan::ThrowError("Must provide a config key to lookup.");
  }

  Nan::Utf8String parameterValue(info[0]->ToString());
  std::string config_key(*parameterValue);

  RdKafka::Conf * topic_conf = topic->m_config;

  std::string value;

  RdKafka::Conf::ConfResult res = topic_conf->get(config_key, value);

  switch (res) {
    case RdKafka::Conf::CONF_UNKNOWN:
      return info.GetReturnValue().Set(Nan::Undefined());
    case RdKafka::Conf::CONF_INVALID:
      return Nan::ThrowError("Topic configuration retroactively invalid...");
    case RdKafka::Conf::CONF_OK:
      return info.GetReturnValue().Set(Nan::New(value).ToLocalChecked());
  }

}

NAN_METHOD(Connection::Topic::NodeGetName) {
  Topic* topic = ObjectWrap::Unwrap<Topic>(info.This());

  info.GetReturnValue().Set(Nan::New(topic->Name()).ToLocalChecked());
}

NAN_METHOD(Connection::Topic::NodePartitionAvailable) {
  // @TODO(sparente)
}

}  // namespace NodeKafka
