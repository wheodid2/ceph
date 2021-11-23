// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
// #hong made new one

#ifndef CEPH_MOSDCONTROLLERQOS_H
#define CEPH_MOSDCONTROLLERQOS_H

#include "include/types.h"
#include "msg/Message.h"


using nreq = double;

class MOSDControllerQoS : public MessageInstance<MOSDControllerQoS> {
public:
  friend factory;

  enum {
    BROADCAST_NREQ_TO_OSD,
    REQUEST_TO_WORK,
    REPLY_TO_LEADER
  };

  const char *get_sub_op_str() const {
    switch(sub_op) {
      case BROADCAST_NREQ_TO_OSD: return "BROADCAST_NREQ_TO_OSD";
      case REQUEST_TO_WORK: return "REQUEST_TO_WORK";
      case REPLY_TO_LEADER: return "REPLY_TO_LEADER";
      default: return "UKNOWN SUB OP";
    }
  }

private:
  int from;
  std::map<uint64_t, nreq> nreq_map;
  __u8 sub_op;

public:
  const std::map<uint64_t, nreq>& get_nreq_map() const { return nreq_map; }
  __u8 get_sub_op() const { return sub_op; }
  int get_from() const {return from; }

//protected:
  MOSDControllerQoS() : MessageInstance(MSG_OSD_CONTROLLER_QOS) {}
  MOSDControllerQoS( const int _from, const std::map<uint64_t, nreq> _nreq_map, const __u8 _sub_op) :
    MessageInstance(MSG_OSD_CONTROLLER_QOS),
    from(_from),
    nreq_map(_nreq_map),
    sub_op(_sub_op)
    {}
  ~MOSDControllerQoS() override {}

public:
  std::string_view get_type_name() const override { return "osd_controller_qos"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(from, payload);
    encode(nreq_map, payload);
    encode(sub_op, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(from, p);
    decode(nreq_map, p);
    decode(sub_op, p);
    ceph_assert(p.end());
  }
};

#endif