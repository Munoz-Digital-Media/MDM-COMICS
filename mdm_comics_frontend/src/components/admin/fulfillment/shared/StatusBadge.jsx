/**
 * StatusBadge - Unified status display for orders, shipments, refunds
 */

import React from 'react';
import {
  Clock, CreditCard, Truck, CheckCircle, X, Package,
  FileText, Tag, AlertTriangle, Ban, DollarSign, RefreshCw, Eye
} from 'lucide-react';

const STATUS_CONFIGS = {
  // Order statuses
  pending: { label: 'Pending', color: 'yellow', icon: Clock },
  paid: { label: 'Paid', color: 'blue', icon: CreditCard },
  shipped: { label: 'Shipped', color: 'purple', icon: Truck },
  delivered: { label: 'Delivered', color: 'green', icon: CheckCircle },
  cancelled: { label: 'Cancelled', color: 'red', icon: X },

  // Shipment statuses
  draft: { label: 'Draft', color: 'gray', icon: FileText },
  label_pending: { label: 'Label Pending', color: 'yellow', icon: Clock },
  label_created: { label: 'Label Created', color: 'blue', icon: Tag },
  picked_up: { label: 'Picked Up', color: 'purple', icon: Package },
  in_transit: { label: 'In Transit', color: 'purple', icon: Truck },
  out_for_delivery: { label: 'Out for Delivery', color: 'blue', icon: Truck },
  exception: { label: 'Exception', color: 'red', icon: AlertTriangle },

  // Refund statuses
  REQUESTED: { label: 'Requested', color: 'yellow', icon: Clock },
  UNDER_REVIEW: { label: 'Under Review', color: 'blue', icon: Eye },
  APPROVED: { label: 'Approved', color: 'green', icon: CheckCircle },
  DENIED: { label: 'Denied', color: 'red', icon: Ban },
  VENDOR_RETURN_INITIATED: { label: 'Return Initiated', color: 'purple', icon: Truck },
  VENDOR_RETURN_IN_TRANSIT: { label: 'Return In Transit', color: 'purple', icon: Truck },
  VENDOR_RETURN_RECEIVED: { label: 'Return Received', color: 'blue', icon: Package },
  VENDOR_CREDIT_PENDING: { label: 'Credit Pending', color: 'yellow', icon: Clock },
  VENDOR_CREDIT_RECEIVED: { label: 'Credit Received', color: 'green', icon: DollarSign },
  CUSTOMER_REFUND_PROCESSING: { label: 'Refund Processing', color: 'blue', icon: RefreshCw },
  CUSTOMER_REFUND_ISSUED: { label: 'Refund Issued', color: 'green', icon: DollarSign },
  COMPLETED: { label: 'Completed', color: 'green', icon: CheckCircle },
  EXCEPTION: { label: 'Exception', color: 'red', icon: AlertTriangle },
};

const colorClasses = {
  yellow: 'bg-yellow-500/20 text-yellow-400',
  blue: 'bg-blue-500/20 text-blue-400',
  purple: 'bg-purple-500/20 text-purple-400',
  green: 'bg-green-500/20 text-green-400',
  red: 'bg-red-500/20 text-red-400',
  orange: 'bg-orange-500/20 text-orange-400',
  gray: 'bg-zinc-500/20 text-zinc-400',
};

export default function StatusBadge({ status, size = 'sm' }) {
  const config = STATUS_CONFIGS[status] || {
    label: status,
    color: 'gray',
    icon: Clock
  };

  const Icon = config.icon;
  const sizeClasses = size === 'sm'
    ? 'px-2 py-0.5 text-xs gap-1'
    : 'px-3 py-1 text-sm gap-1.5';
  const iconSize = size === 'sm' ? 'w-3 h-3' : 'w-4 h-4';

  return (
    <span
      className={`inline-flex items-center rounded-full font-medium ${colorClasses[config.color]} ${sizeClasses}`}
    >
      <Icon className={iconSize} />
      {config.label}
    </span>
  );
}
