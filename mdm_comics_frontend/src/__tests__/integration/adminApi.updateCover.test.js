import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';

import { adminAPI } from '../../services/adminApi';

vi.mock('../../services/api', () => ({
  getStoredToken: vi.fn(() => 'token-123'),
  clearStoredToken: vi.fn(),
}));

describe('adminAPI.updateCover', () => {
  beforeEach(() => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ success: true, s3_url: 'https://example.com/new.jpg' }),
    });
    document.cookie = 'mdm_csrf_token=csrf-abc;';
  });

  afterEach(() => {
    vi.clearAllMocks();
    document.cookie = '';
  });

  test('sends FormData to cover update endpoint with auth headers', async () => {
    const file = new File(['content'], 'cover.jpg', { type: 'image/jpeg' });

    await adminAPI.updateCover(42, file);

    expect(global.fetch).toHaveBeenCalledTimes(1);
    const [url, options] = global.fetch.mock.calls[0];

    expect(url).toMatch(/\/admin\/cover-ingestion\/update\/42$/);
    expect(options.method).toBe('POST');
    expect(options.credentials).toBe('include');

    const formData = options.body;
    expect(formData).toBeInstanceOf(FormData);
    expect(formData.get('file').name).toBe('cover.jpg');

    // Headers should include bearer token and CSRF for mutations
    expect(options.headers.Authorization).toBe('Bearer token-123');
    expect(options.headers['X-CSRF-Token']).toBe('csrf-abc');
  });
});
