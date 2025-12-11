import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import './index.css'

// Analytics imports
import { analytics } from './services/analyticsCollector'
import { replay } from './services/sessionReplay'
import { vitals } from './services/webVitals'

// Initialize analytics on app load
analytics.init()

// Start session replay
replay.start()

// Initialize Web Vitals collection
vitals.init()

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
