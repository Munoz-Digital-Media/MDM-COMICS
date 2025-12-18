import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import MatchCard from '../../components/admin/ingestion/match-review/MatchCard';

const baseMatch = {
  id: 1,
  entity: {
    id: 10,
    type: 'comic',
    name: 'Test Comic',
    series_name: 'Series',
    issue_number: '1',
    publisher: 'Publisher',
    year: 2024,
    isbn: '1234567890',
    upc: '111222333444',
  },
  candidate: {
    source: 'pricecharting',
    id: 'pc-1',
    name: 'Candidate Name',
    price_loose: 1.0,
    price_cib: 2.0,
    price_graded: 3.0,
  },
  match_method: 'fuzzy',
  match_score: 8,
  is_escalated: false,
  can_bulk_approve: true,
};

function renderCard(overrides = {}) {
  const props = {
    match: { ...baseMatch, ...overrides },
    onSelect: vi.fn(),
    onApprove: vi.fn(),
    onReject: vi.fn(),
    onManualSearch: vi.fn(),
    onKeyDown: vi.fn(),
    isProcessing: false,
    tabIndex: 0,
  };

  render(<MatchCard {...props} />);
  return props;
}

describe('MatchCard', () => {
  test('fires approve/reject/manual search actions', async () => {
    const user = userEvent.setup();
    const props = renderCard();

    await user.click(screen.getByRole('button', { name: /approve match/i }));
    await user.click(screen.getByRole('button', { name: /reject match/i }));
    await user.click(screen.getByRole('button', { name: /search manually/i }));

    expect(props.onApprove).toHaveBeenCalledTimes(1);
    expect(props.onReject).toHaveBeenCalledTimes(1);
    expect(props.onManualSearch).toHaveBeenCalledTimes(1);
  });

  test('renders entity and candidate details', () => {
    renderCard();

    expect(screen.getByText(/Test Comic/)).toBeInTheDocument();
    expect(screen.getByText(/Series/)).toBeInTheDocument();
    expect(screen.getByText(/Candidate Name/)).toBeInTheDocument();
    expect(screen.getByText(/Loose/)).toBeInTheDocument();
  });
});
