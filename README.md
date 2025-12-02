# MDM Comics

AI-powered comic book and collectibles e-commerce platform.

## Features

- 🎯 AI-powered CGC grade estimates on ungraded books
- 🛒 Full shopping cart with quantity management
- 🔐 User authentication (login/signup)
- 🔍 Search and filter products
- 📱 Responsive design
- 🎨 MDM brand styling (orange/dark theme)

## Tech Stack

- **Framework:** React 18 + Vite
- **Styling:** Tailwind CSS
- **Icons:** Lucide React
- **Fonts:** Bangers (display), Barlow (body)

## Getting Started

```bash
# Install dependencies
npm install

# Start dev server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## Project Structure

```
mdm_comics/
├── src/
│   ├── App.jsx          # Main application component
│   ├── main.jsx         # React entry point
│   └── index.css        # Global styles + Tailwind
├── index.html           # HTML template
├── package.json
├── vite.config.js
├── tailwind.config.js
└── postcss.config.js
```

## Current Status

**v1.3.0.13** - Frontend prototype with mocked data

### Ready for Backend Integration:
- [ ] Replace `PRODUCTS` array with API calls
- [ ] Implement real authentication
- [ ] Add payment processing (Stripe/PayPal)
- [ ] Build admin dashboard
- [ ] Implement ML grade estimation API

## Brand Colors

- **Primary:** Orange (#F97316)
- **Background:** Zinc-950 (#09090b)
- **Surface:** Zinc-900 (#18181b)
- **Border:** Zinc-800 (#27272a)

## Demo Account

```
Email: demo@mdmcomics.com
Password: demo123
```
