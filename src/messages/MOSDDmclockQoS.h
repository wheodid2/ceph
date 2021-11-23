// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

// #hong
// 
#ifndef CEPH_OSDDMCLOCKQOS_H
#define CEPH_OSDDMCLOCKQOS_H

#include "include/types.h"
#include "msg/Message.h"

class MOSDDmclockQoS : public MessageInstance<MOSDDmclockQoS> {
public:
  friend factory;

  enum {
    DMCLOCK_RESERVATION,
    DMCLOCK_WEIGHT,
    DMCLOCK_LIMIT
  };

  const char *get_sub_op_str() const {
    switch(sub_op) {
      case DMCLOCK_RESERVATION: return "DMCLOCK_RESERVATION";
      case DMCLOCK_WEIGHT: return "DMCLOCK_WEIGHT";
      case DMCLOCK_LIMIT: return "DMCLOCK_LIMIT";
      default: return "UKNOWN SUB OP";
    }
  }

private:
  std::string qos_info;
  __u8 sub_op;

public:
  std::string get_qos_info() const { return qos_info; }
  __u8 get_sub_op() const { return sub_op; }

public:
  MOSDDmclockQoS() : MessageInstance(MSG_OSD_DMCLOCK_QOS) {}
  MOSDDmclockQoS( const std::string _qos_info,
                 const __u8 _sub_op) :
    MessageInstance(MSG_OSD_DMCLOCK_QOS),
    qos_info(_qos_info),
    sub_op(_sub_op)
    {}
  ~MOSDDmclockQoS() override {}

public:
  std::string_view get_type_name() const override { return "osd_dmlock_qos"; }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(qos_info, payload);
    encode(sub_op, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(qos_info, p);
    decode(sub_op, p);
    ceph_assert(p.end());
  }
};

#endif
