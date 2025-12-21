export const DEFAULT_CONVENTIONS = [
  {
    slug: 'galaxycon_columbus',
    name: 'GalaxyCon Columbus',
    baseUrl: 'https://galaxycon.com/pages/galaxycon-columbus',
    parser: 'galaxycon_shopify',
    pages: {
      guests: 'https://galaxycon.com/pages/galaxycon-columbus-guests',
      autographs: 'https://galaxycon.com/pages/galaxycon-columbus-autographs',
      photoOps: 'https://galaxycon.com/pages/galaxycon-columbus-photo-ops',
      groupPhotoOps: 'https://galaxycon.com/pages/galaxycon-columbus-group-photo-ops',
      mailInAutographs: 'https://galaxycon.com/pages/galaxycon-columbus-mail-in-autographs',
    },
    events: [],
  },
  {
    slug: 'frontrowcardshow',
    name: 'Front Row Card Show',
    baseUrl: 'https://frontrowcardshow.com/collections',
    parser: 'frontrow_shopify_collections',
    pages: {
      collections: 'https://frontrowcardshow.com/collections.json',
    },
    events: [
      { name: 'Las Vegas', date_text: 'Jan 10-11, 2026', event_url: 'https://frontrowcardshow.com/collections/las-vegas' },
      { name: 'Pasadena', date_text: 'Jan 31-Feb 1, 2026', event_url: 'https://frontrowcardshow.com/collections/pasadena' },
      { name: 'Phoenix', date_text: 'Dec 27-28, 2026', event_url: 'https://frontrowcardshow.com/collections/phoenix' },
      { name: 'Portland', date_text: 'Feb 28-Mar 1, 2026', event_url: 'https://frontrowcardshow.com/collections/portland' },
      { name: 'San Diego', date_text: 'Jan 17-18, 2026', event_url: 'https://frontrowcardshow.com/collections/san-diego' },
      { name: 'Seattle / Tacoma', date_text: 'Jan 17-18, 2026', event_url: 'https://frontrowcardshow.com/collections/seattle' },
    ],
  },
];
