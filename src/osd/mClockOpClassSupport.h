// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once

#include <bitset>

#include "dmclock/src/dmclock_server.h"
#include "osd/OpRequest.h"
#include "osd/OpQueueItem.h"


namespace ceph {
  namespace mclock {

    using op_item_type_t = OpQueueItem::OpQueueable::op_type_t;
    using VolumeId = uint64_t;
    
    enum class osd_op_type_t {
      client_op, osd_rep_op, bg_snaptrim, bg_recovery, bg_scrub, bg_pg_delete,
      peering_event
    };

    class OpClassClientInfoMgr {
      std::map<VolumeId,crimson::dmclock::ClientInfo> client_infos;
      crimson::dmclock::ClientInfo client_op;
      crimson::dmclock::ClientInfo osd_rep_op;
      crimson::dmclock::ClientInfo snaptrim;
      crimson::dmclock::ClientInfo recov;
      crimson::dmclock::ClientInfo scrub;
      crimson::dmclock::ClientInfo pg_delete;
      crimson::dmclock::ClientInfo peering_event;

      static constexpr std::size_t rep_op_msg_bitset_size = 128;
      std::bitset<rep_op_msg_bitset_size> rep_op_msg_bitset;
      void add_rep_op_msg(int message_code);

    public:

      OpClassClientInfoMgr(CephContext *cct);

      inline crimson::dmclock::ClientInfo*
      get_client_info(VolumeId vid, osd_op_type_t type) {
	switch(type) {
	case osd_op_type_t::client_op: {
	  // Find proper ClientInfo for the specified volume Id
	  auto ret = client_infos.find(vid);
	  if (ret == client_infos.end())
	    return &client_op;
	  else
	    return &(ret->second);
	}
	case osd_op_type_t::osd_rep_op:
	  return &osd_rep_op;
	case osd_op_type_t::bg_snaptrim:
	  return &snaptrim;
	case osd_op_type_t::bg_recovery:
	  return &recov;
	case osd_op_type_t::bg_scrub:
	  return &scrub;
	case osd_op_type_t::bg_pg_delete:
	  return &pg_delete;
	case osd_op_type_t::peering_event:
	  return &peering_event;
	default:
	  ceph_abort();
	  return nullptr;
	}
      }

      // converts operation type from op queue internal to mclock
      // equivalent
      inline static osd_op_type_t convert_op_type(op_item_type_t t) {
	switch(t) {
	case op_item_type_t::client_op:
	  return osd_op_type_t::client_op;
	case op_item_type_t::bg_snaptrim:
	  return osd_op_type_t::bg_snaptrim;
	case op_item_type_t::bg_recovery:
	  return osd_op_type_t::bg_recovery;
	case op_item_type_t::bg_scrub:
	  return osd_op_type_t::bg_scrub;
	case op_item_type_t::bg_pg_delete:
	  return osd_op_type_t::bg_pg_delete;
	case op_item_type_t::peering_event:
	  return osd_op_type_t::peering_event;
	default:
	  ceph_abort();
	}
      }

      inline bool check_client_info(VolumeId vid, osd_op_type_t type) {
	auto ret = client_infos.find(vid);
	  if (ret == client_infos.end())
	    return false;
	  else
	    return true;
      }

	  void add_client_info(VolumeId vid, osd_op_type_t type) {
	crimson::dmclock::ClientInfo client_qos_info(client_op.reservation,
		client_op.weight,
		client_op.limit);
	client_infos.insert(make_pair(vid, client_qos_info));
    }

      osd_op_type_t osd_op_type(const OpQueueItem&) const;

      // used for debugging since faster implementation can be done
      // with rep_op_msg_bitmap
      static bool is_rep_op(uint16_t);
    }; // OpClassClientInfoMgr
  } // namespace mclock
} // namespace ceph
