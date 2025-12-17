/**
 * Feature Flags Configuration
 * Enables compartmentalized feature control for MDM Comics Admin
 *
 * BUNDLE-001: Bundles feature flag for Bundle Builder Tool
 */

// Feature flags can be overridden via environment variables
export const FEATURES = {
  // BUNDLE-001: Bundle Builder Tool - product bundling and combo deals
  BUNDLES_ENABLED: import.meta.env.VITE_FEATURE_BUNDLES !== 'false',

  // BCW Supplies integration
  SUPPLIES_ENABLED: import.meta.env.VITE_FEATURE_SUPPLIES !== 'false',
};

/**
 * Check if a feature is enabled
 * @param {string} featureName - Name of the feature flag
 * @returns {boolean}
 */
export function isFeatureEnabled(featureName) {
  return FEATURES[featureName] ?? false;
}
