import React, { useState } from 'react';
import { MapPin, Phone, Mail, Building2, AlertCircle, CheckCircle, Loader2 } from 'lucide-react';
import { shippingAPI } from '../../services/api';

const US_STATES = [
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
  'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
  'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
  'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
  'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
  'DC', 'PR', 'VI', 'GU', 'AS'
];

export default function AddressForm({
  onAddressCreated,
  onCancel,
  initialData = null,
  addressType = 'shipping'
}) {
  const [formData, setFormData] = useState({
    recipient_name: initialData?.recipient_name || '',
    company_name: initialData?.company_name || '',
    address_line1: initialData?.address_line1 || '',
    address_line2: initialData?.address_line2 || '',
    city: initialData?.city || '',
    state_province: initialData?.state_province || '',
    postal_code: initialData?.postal_code || '',
    country_code: initialData?.country_code || 'US',
    phone: initialData?.phone || '',
    email: initialData?.email || '',
    residential: initialData?.residential ?? true,
    validate_address: true,
    address_type: addressType,
  });

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);
  const [validationResult, setValidationResult] = useState(null);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value,
    }));
    setError(null);
    setValidationResult(null);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);

    try {
      const address = await shippingAPI.createAddress(formData);

      // Check validation status
      if (address.validation_status === 'invalid') {
        setValidationResult({
          type: 'warning',
          message: 'Address could not be validated. Please verify the address is correct.',
        });
      } else if (address.validation_status === 'ambiguous') {
        setValidationResult({
          type: 'warning',
          message: 'Multiple addresses match. The address may need verification.',
        });
      }

      onAddressCreated(address);
    } catch (err) {
      setError(err.message || 'Failed to save address');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {error && (
        <div className="flex items-center gap-2 p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 text-sm">
          <AlertCircle className="w-4 h-4 flex-shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {validationResult && (
        <div className={`flex items-center gap-2 p-3 rounded-lg text-sm ${
          validationResult.type === 'success'
            ? 'bg-green-500/10 border border-green-500/20 text-green-400'
            : 'bg-yellow-500/10 border border-yellow-500/20 text-yellow-400'
        }`}>
          {validationResult.type === 'success'
            ? <CheckCircle className="w-4 h-4 flex-shrink-0" />
            : <AlertCircle className="w-4 h-4 flex-shrink-0" />
          }
          <span>{validationResult.message}</span>
        </div>
      )}

      {/* Recipient Name */}
      <div>
        <label className="block text-sm font-medium text-zinc-300 mb-1">
          Recipient Name *
        </label>
        <div className="relative">
          <input
            type="text"
            name="recipient_name"
            value={formData.recipient_name}
            onChange={handleChange}
            required
            maxLength={100}
            className="w-full px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            placeholder="Full name"
          />
        </div>
      </div>

      {/* Company (Optional) */}
      <div>
        <label className="block text-sm font-medium text-zinc-300 mb-1">
          Company (Optional)
        </label>
        <div className="relative">
          <Building2 className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
          <input
            type="text"
            name="company_name"
            value={formData.company_name}
            onChange={handleChange}
            maxLength={100}
            className="w-full pl-10 pr-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            placeholder="Company name"
          />
        </div>
      </div>

      {/* Address Line 1 */}
      <div>
        <label className="block text-sm font-medium text-zinc-300 mb-1">
          Street Address *
        </label>
        <div className="relative">
          <MapPin className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
          <input
            type="text"
            name="address_line1"
            value={formData.address_line1}
            onChange={handleChange}
            required
            maxLength={100}
            className="w-full pl-10 pr-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            placeholder="Street address"
          />
        </div>
      </div>

      {/* Address Line 2 */}
      <div>
        <label className="block text-sm font-medium text-zinc-300 mb-1">
          Apt, Suite, etc. (Optional)
        </label>
        <input
          type="text"
          name="address_line2"
          value={formData.address_line2}
          onChange={handleChange}
          maxLength={100}
          className="w-full px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
          placeholder="Apartment, suite, unit, etc."
        />
      </div>

      {/* City, State, ZIP */}
      <div className="grid grid-cols-6 gap-3">
        <div className="col-span-3">
          <label className="block text-sm font-medium text-zinc-300 mb-1">
            City *
          </label>
          <input
            type="text"
            name="city"
            value={formData.city}
            onChange={handleChange}
            required
            maxLength={100}
            className="w-full px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            placeholder="City"
          />
        </div>

        <div className="col-span-1">
          <label className="block text-sm font-medium text-zinc-300 mb-1">
            State *
          </label>
          <select
            name="state_province"
            value={formData.state_province}
            onChange={handleChange}
            required
            className="w-full px-2 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white focus:outline-none focus:border-orange-500"
          >
            <option value="">--</option>
            {US_STATES.map(state => (
              <option key={state} value={state}>{state}</option>
            ))}
          </select>
        </div>

        <div className="col-span-2">
          <label className="block text-sm font-medium text-zinc-300 mb-1">
            ZIP Code *
          </label>
          <input
            type="text"
            name="postal_code"
            value={formData.postal_code}
            onChange={handleChange}
            required
            maxLength={10}
            pattern="[0-9]{5}(-[0-9]{4})?"
            className="w-full px-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            placeholder="12345"
          />
        </div>
      </div>

      {/* Phone */}
      <div>
        <label className="block text-sm font-medium text-zinc-300 mb-1">
          Phone (Optional)
        </label>
        <div className="relative">
          <Phone className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
          <input
            type="tel"
            name="phone"
            value={formData.phone}
            onChange={handleChange}
            maxLength={20}
            className="w-full pl-10 pr-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            placeholder="(555) 123-4567"
          />
        </div>
      </div>

      {/* Email */}
      <div>
        <label className="block text-sm font-medium text-zinc-300 mb-1">
          Email (Optional)
        </label>
        <div className="relative">
          <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
          <input
            type="email"
            name="email"
            value={formData.email}
            onChange={handleChange}
            maxLength={100}
            className="w-full pl-10 pr-4 py-2 bg-zinc-800 border border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:outline-none focus:border-orange-500"
            placeholder="email@example.com"
          />
        </div>
      </div>

      {/* Residential Checkbox */}
      <div className="flex items-center gap-2">
        <input
          type="checkbox"
          id="residential"
          name="residential"
          checked={formData.residential}
          onChange={handleChange}
          className="w-4 h-4 rounded border-zinc-700 bg-zinc-800 text-orange-500 focus:ring-orange-500"
        />
        <label htmlFor="residential" className="text-sm text-zinc-300">
          This is a residential address
        </label>
      </div>

      {/* Buttons */}
      <div className="flex gap-3 pt-2">
        {onCancel && (
          <button
            type="button"
            onClick={onCancel}
            disabled={submitting}
            className="flex-1 py-2.5 bg-zinc-700 text-white rounded-lg font-medium hover:bg-zinc-600 transition-colors disabled:opacity-50"
          >
            Cancel
          </button>
        )}
        <button
          type="submit"
          disabled={submitting}
          className="flex-1 py-2.5 bg-orange-500 text-white rounded-lg font-medium hover:bg-orange-600 transition-colors disabled:opacity-50 flex items-center justify-center gap-2"
        >
          {submitting ? (
            <>
              <Loader2 className="w-4 h-4 animate-spin" />
              Saving...
            </>
          ) : (
            'Save Address'
          )}
        </button>
      </div>
    </form>
  );
}
