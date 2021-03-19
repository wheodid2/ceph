// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MMDSCONTROLLERQOS_H
#define CEPH_MMDSCONTROLLERQOS_H

#include "include/types.h"
#include "msg/Message.h"

using VolumeId = std::string;
using GVF = double;

class MMDSControllerQoS : public MessageInstance<MMDSControllerQoS> {
public:
  friend factory;

  enum {
    REQUEST_VCNT_TO_WRKR,
    REQUEST_TO_CTRL_SIM,
    REQUEST_TO_WRKR_SIM
  };

  const char *get_sub_op_str() const {
    switch(sub_op) {
      case REQUEST_VCNT_TO_WRKR: return "REQUEST_VCNT_TO_WRKR";
      case REQUEST_TO_CTRL_SIM: return "REQUEST_TO_CTRL_SIM";
      case REQUEST_TO_WRKR_SIM: return "REQUEST_TO_WRKR_SIM";
      default: return "UKNOWN CONTROLLER SUB OP";
    }
  }

private:
  mds_rank_t from;
  std::map<VolumeId, GVF> gvf_map;
  __u8 sub_op;

public:
  mds_rank_t get_mds_from() const { return from; }
  const std::map<VolumeId, GVF>& get_gvf_map() const { return gvf_map; }
  __u8 get_sub_op() const { return sub_op; }

protected:
  MMDSControllerQoS() : MessageInstance(MSG_MDS_CONTROLLER_QOS) {}
  MMDSControllerQoS(const mds_rank_t _from, const std::map<VolumeId, GVF> _gvf_map, const __u8 _sub_op) :
    MessageInstance(MSG_MDS_CONTROLLER_QOS),
    from(_from),
    gvf_map(_gvf_map),
    sub_op(_sub_op)
    {}
  ~MMDSControllerQoS() override {}

public:
  std::string_view get_type_name() const override { return "mds_dmlock_qos"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(from, payload);
    encode(gvf_map, payload);
    encode(sub_op, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(from, p);
    decode(gvf_map, p);
    decode(sub_op, p);
    ceph_assert(p.end());
  }
};

#endif
