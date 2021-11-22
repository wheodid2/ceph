// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <memory>

#include "osd/mClockClientQueue.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


namespace ceph {

  /*
   * class mClockClientQueue
   */

  mClockClientQueue::mClockClientQueue(CephContext *cct) :
    queue(std::bind(&mClockClientQueue::op_class_client_info_f, this, _1),
	  cct->_conf->osd_op_queue_mclock_anticipation_timeout),
    client_info_mgr(cct)
  {
    // empty
    osd_shard_cct = cct;
  }

  const dmc::ClientInfo* mClockClientQueue::op_class_client_info_f(
    const mClockClientQueue::InnerClient& client)
  {
    return client_info_mgr.get_client_info(client.first, client.second);
  }

  mClockClientQueue::InnerClient
  inline mClockClientQueue::get_inner_client(const Client& cl,
					     const Request& request) {
    return InnerClient(cl, client_info_mgr.osd_op_type(request));
  }


  inline void mClockClientQueue::update_qos_info(Client cl,
  						 int qos_type,
						 double qos_val) {
    dmc::ClientInfo* qos_info;
    if (client_info_mgr.check_client_info(cl, osd_op_type_t::client_op)) {
      qos_info = client_info_mgr.get_client_info(cl, osd_op_type_t::client_op);
      ldout(osd_shard_cct, 20) << "update_qos_info::check_client_info::true volume_id: " << cl << dendl;
    }
    else {
      client_info_mgr.add_client_info(cl, osd_op_type_t::client_op);
      qos_info = client_info_mgr.get_client_info(cl, osd_op_type_t::client_op);
      ldout(osd_shard_cct, 20) << "update_qos_info::check_client_info::false: add_new_client volume_id: " << cl << dendl;
    }
    switch(qos_type) {
      case 0:
        qos_info->update(qos_val, qos_info->weight, qos_info->limit);
	break;
      case 1:
        qos_info->update(qos_info->reservation, qos_val, qos_info->limit);
	break;
      case 2:
        qos_info->update(qos_info->reservation, qos_info->weight, qos_val);
	break;
      default:
        break;
    }
    queue.update_qos_info(InnerClient(cl, osd_op_type_t::client_op), qos_type, qos_val);
    ldout(osd_shard_cct, 20) << "update_qos_info::upadted_qos_info " << *qos_info << dendl;
  }

  // Formatted output of the queue
  inline void mClockClientQueue::dump(ceph::Formatter *f) const {
    queue.dump(f);
  }

  inline void mClockClientQueue::enqueue_strict(Client cl,
						unsigned priority,
						Request&& item) {
    queue.enqueue_strict(get_inner_client(cl, item), priority,
			 std::move(item));
  }

  // Enqueue op in the front of the strict queue
  inline void mClockClientQueue::enqueue_strict_front(Client cl,
						      unsigned priority,
						      Request&& item) {
    queue.enqueue_strict_front(get_inner_client(cl, item), priority,
			       std::move(item));
  }

  // Enqueue op in the back of the regular queue
  inline void mClockClientQueue::enqueue(Client cl,
					 unsigned priority,
					 unsigned cost,
					 Request&& item) {
    queue.enqueue(get_inner_client(cl, item), priority, 1u, std::move(item));
  }

  // Enqueue the op in the front of the regular queue
  inline void mClockClientQueue::enqueue_front(Client cl,
					       unsigned priority,
					       unsigned cost,
					       Request&& item) {
    queue.enqueue_front(get_inner_client(cl, item), priority, 1u,
			std::move(item));
  }

  inline void mClockClientQueue::enqueue_gvf(Client cl,
					 unsigned priority,
					 unsigned cost,
					 Request&& item,
           double gvf) {
    queue.enqueue_gvf(get_inner_client(cl, item), priority, 1u, std::move(item), gvf);
  }

  // Return an op to be dispatched
  inline WorkItem mClockClientQueue::dequeue() {
    return queue.dequeue();
  }
} // namespace ceph
