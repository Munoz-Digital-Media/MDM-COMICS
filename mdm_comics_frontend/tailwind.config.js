/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // MDM Brand Colors
        'mdm-orange': {
          400: '#fb923c',
          500: '#f97316',
          600: '#ea580c',
        }
      },
      fontFamily: {
        'comic': ['Bangers', 'cursive'],
        'body': ['Barlow', 'sans-serif'],
      }
    },
  },
  plugins: [],
}
