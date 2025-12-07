/**
 * AboutContact - Combined About & Contact Modal
 * IMPL-001: About & Contact Page Implementation
 *
 * Features:
 * - Store information and mission statement
 * - Contact form with validation
 * - Accessible: keyboard navigation, focus trap, ARIA labels
 * - Prefers-reduced-motion support
 */

import React, { useState, useEffect, useRef } from 'react';
import { X, Send, CheckCircle, AlertCircle, Loader2, Clock, Mail, MapPin, Info } from 'lucide-react';
import { contactAPI } from '../services/api';

const SUBJECT_OPTIONS = [
  { value: 'general', label: 'General Inquiry' },
  { value: 'order', label: 'Order Question' },
  { value: 'returns', label: 'Returns & Exchanges' },
  { value: 'wholesale', label: 'Wholesale Inquiry' },
  { value: 'other', label: 'Other' },
];

export default function AboutContact({ onClose }) {
  const [activeTab, setActiveTab] = useState('about');
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    subject: 'general',
    message: '',
  });
  const [errors, setErrors] = useState({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitStatus, setSubmitStatus] = useState(null); // 'success' | 'error' | null
  const [referenceId, setReferenceId] = useState(null);

  const modalRef = useRef(null);
  const firstFocusableRef = useRef(null);

  // Focus trap and keyboard handling
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        onClose();
      }

      // Focus trap
      if (e.key === 'Tab' && modalRef.current) {
        const focusable = modalRef.current.querySelectorAll(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
        );
        const first = focusable[0];
        const last = focusable[focusable.length - 1];

        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    firstFocusableRef.current?.focus();

    // Prevent body scroll
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = '';
    };
  }, [onClose]);

  // Form validation
  const validateForm = () => {
    const newErrors = {};

    if (!formData.name.trim()) {
      newErrors.name = 'Name is required';
    } else if (formData.name.trim().length > 100) {
      newErrors.name = 'Name must be 100 characters or less';
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!formData.email.trim()) {
      newErrors.email = 'Email is required';
    } else if (!emailRegex.test(formData.email)) {
      newErrors.email = 'Please enter a valid email address';
    }

    if (!formData.message.trim()) {
      newErrors.message = 'Message is required';
    } else if (formData.message.trim().length < 10) {
      newErrors.message = 'Message must be at least 10 characters';
    } else if (formData.message.trim().length > 2000) {
      newErrors.message = 'Message must be 2000 characters or less';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData((prev) => ({ ...prev, [name]: value }));
    // Clear error when user starts typing
    if (errors[name]) {
      setErrors((prev) => ({ ...prev, [name]: null }));
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!validateForm()) return;

    setIsSubmitting(true);
    setSubmitStatus(null);

    try {
      const response = await contactAPI.submit(formData);
      setSubmitStatus('success');
      setReferenceId(response.reference_id);
      // Clear form on success
      setFormData({
        name: '',
        email: '',
        subject: 'general',
        message: '',
      });
    } catch (error) {
      setSubmitStatus('error');
      console.error('Contact form submission failed:', error);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="about-contact-title"
    >
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/70 backdrop-blur-sm motion-safe:animate-fadeIn"
        onClick={onClose}
        aria-hidden="true"
      />

      {/* Modal Panel */}
      <div
        ref={modalRef}
        className="relative bg-zinc-900 rounded-2xl border border-zinc-800 w-full max-w-lg max-h-[90vh] overflow-hidden motion-safe:animate-slideUp"
      >
        {/* Header */}
        <div className="p-6 border-b border-zinc-800">
          <div className="flex items-center justify-between">
            <h2 id="about-contact-title" className="text-xl font-bold text-white">
              {activeTab === 'about' ? 'About MDM Comics' : 'Contact Us'}
            </h2>
            <button
              ref={firstFocusableRef}
              onClick={onClose}
              className="p-2 text-zinc-400 hover:text-white hover:bg-zinc-800 rounded-lg transition-colors"
              aria-label="Close modal"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Tab Navigation */}
          <div className="flex gap-2 mt-4">
            <button
              onClick={() => setActiveTab('about')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                activeTab === 'about'
                  ? 'bg-orange-500 text-white'
                  : 'bg-zinc-800 text-zinc-400 hover:text-white'
              }`}
              aria-pressed={activeTab === 'about'}
            >
              <Info className="w-4 h-4 inline mr-2" />
              About
            </button>
            <button
              onClick={() => setActiveTab('contact')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                activeTab === 'contact'
                  ? 'bg-orange-500 text-white'
                  : 'bg-zinc-800 text-zinc-400 hover:text-white'
              }`}
              aria-pressed={activeTab === 'contact'}
            >
              <Mail className="w-4 h-4 inline mr-2" />
              Contact
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="p-6 overflow-y-auto max-h-[calc(90vh-180px)]">
          {activeTab === 'about' ? (
            <AboutSection />
          ) : (
            <ContactForm
              formData={formData}
              errors={errors}
              isSubmitting={isSubmitting}
              submitStatus={submitStatus}
              referenceId={referenceId}
              onInputChange={handleInputChange}
              onSubmit={handleSubmit}
              onResetStatus={() => setSubmitStatus(null)}
            />
          )}
        </div>
      </div>

      {/* Styles for animations */}
      <style>{`
        @keyframes fadeIn {
          from { opacity: 0; }
          to { opacity: 1; }
        }
        @keyframes slideUp {
          from {
            opacity: 0;
            transform: translateY(20px) scale(0.95);
          }
          to {
            opacity: 1;
            transform: translateY(0) scale(1);
          }
        }
        .motion-safe\\:animate-fadeIn {
          animation: fadeIn 0.2s ease-out;
        }
        .motion-safe\\:animate-slideUp {
          animation: slideUp 0.3s ease-out;
        }
        @media (prefers-reduced-motion: reduce) {
          .motion-safe\\:animate-fadeIn,
          .motion-safe\\:animate-slideUp {
            animation: none;
          }
        }
      `}</style>
    </div>
  );
}

// About Section Component
function AboutSection() {
  return (
    <div className="space-y-6">
      {/* Mission */}
      <div>
        <p className="text-xl text-orange-400 font-semibold mb-3">
          Slabs for the serious. Back issues for the curious. Funkos for everyone!
        </p>
        <div className="space-y-3 text-zinc-300">
          <p>
            MDM Comics is your trusted source for comic books, graded collectibles, Funko POPs, and collector supplies.
          </p>
          <p>
            Whether you are hunting for a key issue slab, filling gaps in your collection with quality back issues, or adding to your Funko shelf - we have got you covered.
          </p>
          <p>
            We are an online-only store, shipping nationwide with secure packaging and insurance options for high-value items.
          </p>
        </div>
      </div>

      {/* Store Info Cards */}
      <div className="grid gap-4">
        <div className="flex items-start gap-3 p-4 bg-zinc-800/50 rounded-xl">
          <div className="w-10 h-10 bg-orange-500/20 rounded-lg flex items-center justify-center flex-shrink-0">
            <MapPin className="w-5 h-5 text-orange-400" />
          </div>
          <div>
            <h3 className="font-semibold text-white mb-1">Location</h3>
            <p className="text-zinc-400 text-sm">
              Online store open 24/7
            </p>
          </div>
        </div>

        <div className="flex items-start gap-3 p-4 bg-zinc-800/50 rounded-xl">
          <div className="w-10 h-10 bg-orange-500/20 rounded-lg flex items-center justify-center flex-shrink-0">
            <Clock className="w-5 h-5 text-orange-400" />
          </div>
          <div>
            <h3 className="font-semibold text-white mb-1">Support Hours</h3>
            <p className="text-zinc-400 text-sm">
              Mon-Fri 9am-5pm PST
            </p>
          </div>
        </div>

        <div className="flex items-start gap-3 p-4 bg-zinc-800/50 rounded-xl">
          <div className="w-10 h-10 bg-orange-500/20 rounded-lg flex items-center justify-center flex-shrink-0">
            <Mail className="w-5 h-5 text-orange-400" />
          </div>
          <div>
            <h3 className="font-semibold text-white mb-1">Contact</h3>
            <p className="text-zinc-400 text-sm">
              Use the Contact tab to send us a message
            </p>
          </div>
        </div>
      </div>

      {/* Social links reference */}
      <p className="text-sm text-zinc-500 text-center">
        Follow us on social media - links in the footer below
      </p>
    </div>
  );
}

// Contact Form Component
function ContactForm({
  formData,
  errors,
  isSubmitting,
  submitStatus,
  referenceId,
  onInputChange,
  onSubmit,
  onResetStatus,
}) {
  // Success state
  if (submitStatus === 'success') {
    return (
      <div className="text-center py-8">
        <div className="w-16 h-16 bg-green-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
          <CheckCircle className="w-8 h-8 text-green-400" />
        </div>
        <h3 className="text-xl font-semibold text-white mb-2">Message Sent!</h3>
        <p className="text-zinc-400 mb-2">
          Thank you for reaching out. We will get back to you within 1-2 business days.
        </p>
        {referenceId && (
          <p className="text-sm text-zinc-500">
            Reference: <span className="font-mono text-orange-400">{referenceId}</span>
          </p>
        )}
        <button
          onClick={onResetStatus}
          className="mt-6 px-6 py-2 bg-zinc-800 text-zinc-300 rounded-lg hover:bg-zinc-700 transition-colors"
        >
          Send Another Message
        </button>
      </div>
    );
  }

  // Error state
  if (submitStatus === 'error') {
    return (
      <div className="text-center py-8">
        <div className="w-16 h-16 bg-red-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
          <AlertCircle className="w-8 h-8 text-red-400" />
        </div>
        <h3 className="text-xl font-semibold text-white mb-2">Something went wrong</h3>
        <p className="text-zinc-400 mb-4">
          We could not send your message. Please try again or email us directly.
        </p>
        <button
          onClick={onResetStatus}
          className="px-6 py-2 bg-orange-500 text-white rounded-lg hover:bg-orange-600 transition-colors"
        >
          Try Again
        </button>
      </div>
    );
  }

  return (
    <form onSubmit={onSubmit} className="space-y-4">
      {/* Name */}
      <div>
        <label htmlFor="contact-name" className="block text-sm font-medium text-zinc-300 mb-1">
          Your Name *
        </label>
        <input
          id="contact-name"
          type="text"
          name="name"
          value={formData.name}
          onChange={onInputChange}
          placeholder="John Doe"
          maxLength={100}
          className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none transition-colors ${
            errors.name
              ? 'border-red-500 focus:border-red-500'
              : 'border-zinc-700 focus:border-orange-500'
          }`}
          aria-invalid={errors.name ? 'true' : 'false'}
          aria-describedby={errors.name ? 'name-error' : undefined}
        />
        {errors.name && (
          <p id="name-error" className="mt-1 text-sm text-red-400 flex items-center gap-1">
            <AlertCircle className="w-3 h-3" />
            {errors.name}
          </p>
        )}
      </div>

      {/* Email */}
      <div>
        <label htmlFor="contact-email" className="block text-sm font-medium text-zinc-300 mb-1">
          Email Address *
        </label>
        <input
          id="contact-email"
          type="email"
          name="email"
          value={formData.email}
          onChange={onInputChange}
          placeholder="john@example.com"
          maxLength={255}
          className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none transition-colors ${
            errors.email
              ? 'border-red-500 focus:border-red-500'
              : 'border-zinc-700 focus:border-orange-500'
          }`}
          aria-invalid={errors.email ? 'true' : 'false'}
          aria-describedby={errors.email ? 'email-error' : undefined}
        />
        {errors.email && (
          <p id="email-error" className="mt-1 text-sm text-red-400 flex items-center gap-1">
            <AlertCircle className="w-3 h-3" />
            {errors.email}
          </p>
        )}
      </div>

      {/* Subject */}
      <div>
        <label htmlFor="contact-subject" className="block text-sm font-medium text-zinc-300 mb-1">
          Subject *
        </label>
        <select
          id="contact-subject"
          name="subject"
          value={formData.subject}
          onChange={onInputChange}
          className="w-full px-4 py-3 bg-zinc-800 border border-zinc-700 rounded-xl text-white focus:outline-none focus:border-orange-500 transition-colors"
        >
          {SUBJECT_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>

      {/* Message */}
      <div>
        <label htmlFor="contact-message" className="block text-sm font-medium text-zinc-300 mb-1">
          Message *
        </label>
        <textarea
          id="contact-message"
          name="message"
          value={formData.message}
          onChange={onInputChange}
          placeholder="How can we help?"
          rows={5}
          maxLength={2000}
          className={`w-full px-4 py-3 bg-zinc-800 border rounded-xl text-white placeholder-zinc-500 focus:outline-none transition-colors resize-none ${
            errors.message
              ? 'border-red-500 focus:border-red-500'
              : 'border-zinc-700 focus:border-orange-500'
          }`}
          aria-invalid={errors.message ? 'true' : 'false'}
          aria-describedby={errors.message ? 'message-error' : 'message-hint'}
        />
        <div className="flex justify-between mt-1">
          {errors.message ? (
            <p id="message-error" className="text-sm text-red-400 flex items-center gap-1">
              <AlertCircle className="w-3 h-3" />
              {errors.message}
            </p>
          ) : (
            <p id="message-hint" className="text-xs text-zinc-500">
              {formData.message.length}/2000 characters
            </p>
          )}
        </div>
      </div>

      {/* Submit Button */}
      <button
        type="submit"
        disabled={isSubmitting}
        className="w-full py-3 bg-orange-500 rounded-xl font-semibold text-white hover:bg-orange-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
      >
        {isSubmitting ? (
          <>
            <Loader2 className="w-5 h-5 animate-spin" />
            Sending...
          </>
        ) : (
          <>
            <Send className="w-5 h-5" />
            Send Message
          </>
        )}
      </button>
    </form>
  );
}
