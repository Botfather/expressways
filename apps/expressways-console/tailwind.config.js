/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        heading: ['"Space Grotesk"', 'sans-serif'],
        body: ['"IBM Plex Sans"', 'sans-serif'],
        mono: ['"IBM Plex Mono"', 'monospace'],
      },
      colors: {
        ink: '#112A46',
        mist: '#D9E8F5',
        paper: '#F6FBFF',
        signal: '#F05A28',
        leaf: '#1F7A53',
      },
      keyframes: {
        drift: {
          '0%, 100%': { transform: 'translateY(0px)' },
          '50%': { transform: 'translateY(-8px)' },
        },
        fadeup: {
          from: { opacity: '0', transform: 'translateY(12px)' },
          to: { opacity: '1', transform: 'translateY(0px)' },
        },
      },
      animation: {
        drift: 'drift 6s ease-in-out infinite',
        fadeup: 'fadeup 450ms ease-out both',
      },
    },
  },
  plugins: [],
}

