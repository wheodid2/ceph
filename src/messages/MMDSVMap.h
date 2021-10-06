// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
// #hong made new one

#ifndef CEPH_MMDSVMap_H
#define CEPH_MMDSVMap_H

#include "include/types.h"
#include "msg/Message.h"

// something can be added 
// what is the type of volume_id:session_id?
using volm_id = std::string;
using ssin_id = std::string;

// using nreq = double;

class MMDSVMap : public MessageInstance<MMDSVMap> {
public:
  friend factory;

  enum {
    BROADCAST_NREQ_TO_OSD,
    REQUEST_TO_WORK,
    REPLY_TO_LEADER,
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
  std::map<volm_id, std::set<ssin_id>> vi_si_map;
  __u8 sub_op;

public:
  const std::map<volm_id, std::set<ssin_id>>& get_vi_si_map() const { return vi_si_map; }
  __u8 get_sub_op() const { return sub_op; }
  int get_from() const {return from; }

//protected:
  MMDSVMap() : MessageInstance(MSG_MDS_MONITOR_VMAP) {}
  MMDSVMap( const int _from, const std::map<volm_id, std::set<ssin_id>> _vi_si_map, const __u8 _sub_op) :
    from(_from),
    MessageInstance(MSG_MDS_MONITOR_VMAP),
    vi_si_map(_vi_si_map),
    sub_op(_sub_op)
    {}
  ~MMDSVMap() override {}

public:
  std::string_view get_type_name() const override { return "mds_monitor_vmap"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(from, payload);
    encode(vi_si_map, payload);
    encode(sub_op, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(from, p);
    decode(vi_si_map, p);
    decode(sub_op, p);
    ceph_assert(p.end());
  }
};

#endif