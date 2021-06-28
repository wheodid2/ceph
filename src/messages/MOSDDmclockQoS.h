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
    REQUEST_TO_AUTH,
    BROADCAST_TO_ALL,
    REPLY_TO_LEADER,
    PATH_TRAVERSE
  };

  const char *get_sub_op_str() const {
    switch(sub_op) {
      case REQUEST_TO_AUTH: return "REQUEST_TO_AUTH";
      case BROADCAST_TO_ALL: return "BROADCAST_TO_ALL";
      case REPLY_TO_LEADER: return "REPLY_TO_LEADER";
      case PATH_TRAVERSE: return "PATH_TRAVERSE";
      default: return "UKNOWN SUB OP";
    }
  }

private:
  //mds_rank_t from;     // should change #hong
  //std::string volume_id;   // #hong: we don't know it
  //dmclock_osd_info_t dmclock_osd_info;  // #hong should change
  __u8 sub_op;

public:
  //mds_rank_t get_mds_from() const { return from; }    // should change #hong
  //std::string get_volume_id() const { return volume_id; } // should change #hong
  //const dmclock_osd_info_t& get_dmclock_osd_info() const { return dmclock_osd_info; }   // 
  __u8 get_sub_op() const { return sub_op; }

public:
  MOSDDmclockQoS() : MessageInstance(MSG_OSD_DMCLOCK_QOS) {}
  MOSDDmclockQoS(
                 const __u8 _sub_op) :      // delete rank #hong
    MessageInstance(MSG_OSD_DMCLOCK_QOS),
    sub_op(_sub_op)
    {}
  ~MOSDDmclockQoS() override {}

public:
  std::string_view get_type_name() const override { return "osd_dmlock_qos"; }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    //encode(volume_id, payload);
    //encode(dmclock_osd_info, payload);
    encode(sub_op, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    //decode(volume_id, p);
    //decode(dmclock_osd_info, p);
    decode(sub_op, p);
    ceph_assert(p.end());
  }
};

#endif
