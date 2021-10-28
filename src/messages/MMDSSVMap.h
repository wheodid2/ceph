// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
// #hong made new one

#ifndef CEPH_MMDSSVMap_H
#define CEPH_MMDSSVMap_H

#include "include/types.h"
#include "msg/Message.h"

class MMDSSVMap : public MessageInstance<MMDSSVMap> {
public:
  friend factory;

private:
  int from;
  std::string session_id;
  std::string volume_id;

public:
  int get_from() const { return from; }
  std::string get_volume_id() const { return volume_id; }
  std::string get_session_id() const { return session_id; }

protected:
  MMDSSVMap() : MessageInstance(MSG_MDS_MONITOR_SVMAP) {}
  MMDSSVMap(const int _from, const std::string _session_id, const std::string _volume_id) :
    MessageInstance(MSG_MDS_MONITOR_SVMAP),
    from(_from),
    session_id(_session_id),
    volume_id(_volume_id)
    {}
  ~MMDSSVMap() override {}

public:
  std::string_view get_type_name() const override { return "mds_monitor_svmap"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(from, payload);
    encode(session_id, payload);
    encode(volume_id, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(from, p);
    decode(session_id, p);
    decode(volume_id, p);
    ceph_assert(p.end());
  }
};

#endif