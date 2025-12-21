/**
 * Component tests for ConventionQuickAccess
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ConventionQuickAccess from '../../components/ConventionQuickAccess';

// Mock the conventions config
vi.mock('../../config/conventions.config', () => ({
  DEFAULT_CONVENTIONS: [
    {
      slug: 'testcon',
      name: 'Test Convention',
      baseUrl: 'https://testcon.com',
      events: [
        { name: 'Las Vegas', date_text: 'Jan 10-11, 2030', event_url: 'https://testcon.com/vegas' },
        { name: 'Phoenix', date_text: 'Dec 27-28, 2030', event_url: 'https://testcon.com/phoenix' },
        { name: 'San Diego', date_text: 'Jan 17-18, 2030', event_url: 'https://testcon.com/sandiego' },
      ],
    },
  ],
}));

describe('ConventionQuickAccess', () => {
  beforeEach(() => {
    // Mock current date to Jan 1, 2030 (before all test events)
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2030, 0, 1, 12, 0, 0));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('renders convention buttons', () => {
    render(<ConventionQuickAccess />);

    expect(screen.getByText('Las Vegas')).toBeInTheDocument();
    expect(screen.getByText('Phoenix')).toBeInTheDocument();
    expect(screen.getByText('San Diego')).toBeInTheDocument();
  });

  it('renders section label', () => {
    render(<ConventionQuickAccess />);

    expect(screen.getByText('Upcoming Conventions')).toBeInTheDocument();
  });

  it('sorts buttons by date ascending (earliest first)', () => {
    render(<ConventionQuickAccess />);

    const buttons = screen.getAllByRole('tab');
    // Jan 10-11 should be first, then Jan 17-18, then Dec 27-28
    expect(buttons[0]).toHaveTextContent('Las Vegas');
    expect(buttons[1]).toHaveTextContent('San Diego');
    expect(buttons[2]).toHaveTextContent('Phoenix');
  });

  it('expands detail card on button click', () => {
    render(<ConventionQuickAccess />);

    const vegasButton = screen.getByText('Las Vegas').closest('button');
    fireEvent.click(vegasButton);

    // Detail card should appear with convention name
    expect(screen.getByText('Test Convention')).toBeInTheDocument();
    expect(screen.getByText('View Details')).toBeInTheDocument();
  });

  it('collapses card on second click', () => {
    render(<ConventionQuickAccess />);

    const vegasButton = screen.getByText('Las Vegas').closest('button');

    // First click - expand
    fireEvent.click(vegasButton);
    expect(screen.getByText('Test Convention')).toBeInTheDocument();

    // Second click - collapse
    fireEvent.click(vegasButton);
    expect(screen.queryByText('View Details')).not.toBeInTheDocument();
  });

  it('switches expanded card when clicking different button', () => {
    render(<ConventionQuickAccess />);

    const vegasButton = screen.getByText('Las Vegas').closest('button');
    const phoenixButton = screen.getByText('Phoenix').closest('button');

    // Click Vegas
    fireEvent.click(vegasButton);
    expect(screen.getByRole('tabpanel')).toHaveTextContent('Las Vegas');

    // Click Phoenix - should switch
    fireEvent.click(phoenixButton);
    expect(screen.getByRole('tabpanel')).toHaveTextContent('Phoenix');
  });

  it('renders external link with correct attributes', () => {
    render(<ConventionQuickAccess />);

    const vegasButton = screen.getByText('Las Vegas').closest('button');
    fireEvent.click(vegasButton);

    const titleLink = screen.getByRole('link', { name: /visit test convention website/i });
    expect(titleLink).toHaveAttribute('href', 'https://testcon.com/vegas');
    expect(titleLink).toHaveAttribute('target', '_blank');
    expect(titleLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('renders View Details button with correct link', () => {
    render(<ConventionQuickAccess />);

    const vegasButton = screen.getByText('Las Vegas').closest('button');
    fireEvent.click(vegasButton);

    const detailsLink = screen.getByText('View Details').closest('a');
    expect(detailsLink).toHaveAttribute('href', 'https://testcon.com/vegas');
    expect(detailsLink).toHaveAttribute('target', '_blank');
  });

  it('has correct aria attributes on buttons', () => {
    render(<ConventionQuickAccess />);

    const vegasButton = screen.getByText('Las Vegas').closest('button');
    expect(vegasButton).toHaveAttribute('role', 'tab');
    expect(vegasButton).toHaveAttribute('aria-selected', 'false');
    expect(vegasButton).toHaveAttribute('aria-expanded', 'false');

    // Click to expand
    fireEvent.click(vegasButton);
    expect(vegasButton).toHaveAttribute('aria-selected', 'true');
    expect(vegasButton).toHaveAttribute('aria-expanded', 'true');
  });

  it('renders date badge on buttons', () => {
    render(<ConventionQuickAccess />);

    // Date without year should be shown
    expect(screen.getByText('Jan 10-11')).toBeInTheDocument();
    expect(screen.getByText('Jan 17-18')).toBeInTheDocument();
    expect(screen.getByText('Dec 27-28')).toBeInTheDocument();
  });

  it('shows full date in expanded card', () => {
    render(<ConventionQuickAccess />);

    const vegasButton = screen.getByText('Las Vegas').closest('button');
    fireEvent.click(vegasButton);

    // Full date with year in expanded card
    expect(screen.getByText('Jan 10-11, 2030')).toBeInTheDocument();
  });
});

describe('ConventionQuickAccess - Edge Cases', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.resetModules();
  });

  it('renders nothing when all events are in the past', async () => {
    // Mock date to be after all events
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2031, 0, 1));

    // Re-mock with past dates
    vi.doMock('../../config/conventions.config', () => ({
      DEFAULT_CONVENTIONS: [
        {
          slug: 'testcon',
          name: 'Test Convention',
          baseUrl: 'https://testcon.com',
          events: [
            { name: 'Past Event', date_text: 'Jan 10-11, 2020', event_url: 'https://testcon.com/past' },
          ],
        },
      ],
    }));

    // Component should return null (no content)
    const { container } = render(<ConventionQuickAccess />);
    // When no future events, component returns null
    expect(container.querySelector('section')).toBeNull();
  });
});
