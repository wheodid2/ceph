// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDSDMCLOCKQOS_H
#define CEPH_MDSDMCLOCKQOS_H

#include "include/types.h"
#include "msg/Message.h"

class MDSDmclockQoS : public MessageInstance<MDSDmclockQoS> {
public:
  friend factory;

  enum {
    REQUEST_TO_AUTH,
    BROADCAST_TO_ALL,
    PATH_TRAVERSE,
    REQUEST_TO_CTRL_SIM,
    REQUEST_TO_WRKR_SIM
  };

  const char *get_sub_op_str() const {
    switch(sub_op) {
      case REQUEST_TO_AUTH: return "REQUEST_TO_AUTH";
      case BROADCAST_TO_ALL: return "BROADCAST_TO_ALL";
      case PATH_TRAVERSE: return "PATH_TRAVERSE";
      case REQUEST_TO_CTRL_SIM: return "REQUEST_TO_CTRL_SIM";
      case REQUEST_TO_WRKR_SIM: return "REQUEST_TO_WRKR_SIM";
      default: return "UKNOWN SUB OP";
    }
  }

private:
  mds_rank_t from;
  std::string volume_id;
  dmclock_info_t dmclock_info;
  __u8 sub_op;

public:
  mds_rank_t get_mds_from() const { return from; }
  std::string get_volume_id() const { return volume_id; }
  const dmclock_info_t& get_dmclock_info() const { return dmclock_info; }
  __u8 get_sub_op() const { return sub_op; }

protected:
  MDSDmclockQoS() : MessageInstance(MSG_MDS_DMCLOCK_QOS) {}
  MDSDmclockQoS(const mds_rank_t _from, const std::string& _volume_id,
                const dmclock_info_t& _dmclock_info, const __u8 _sub_op) :
    MessageInstance(MSG_MDS_DMCLOCK_QOS),
    from(_from),
    volume_id(_volume_id),
    dmclock_info(_dmclock_info),
    sub_op(_sub_op)
    {}
  ~MDSDmclockQoS() override {}

public:
  std::string_view get_type_name() const override { return "mds_dmlock_qos"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(from, payload);
    encode(volume_id, payload);
    encode(dmclock_info, payload);
    encode(sub_op, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(from, p);
    decode(volume_id, p);
    decode(dmclock_info, p);
    decode(sub_op, p);
    ceph_assert(p.end());
  }
};

#endif
