import { Link } from "react-router-dom";
import "../styles/landing.css";

export default function Landing() {
  return (
    <div className="landing">
      <nav className="landing-nav">
        <span className="landing-brand">SHIELD</span>
        <Link className="nav-link" to="/dashboard">
          Dashboard →
        </Link>
      </nav>

      <header className="landing-hero">
        <h1>Weather-aware sequence risk for DFW hub pairs</h1>
        <p className="lead">
          Explore forecast horizons, month-specific pair risk, and integrated scores that blend operational
          multitask models with predicted weather at both endpoints of <code>A → DFW → B</code>.
        </p>
        <div className="landing-cta">
          <Link to="/dashboard" className="btn-primary">
            Open dashboard
          </Link>
          <a href="#features" className="btn-ghost">
            How it works
          </a>
        </div>
      </header>

      <section className="landing-features" id="features">
        <div className="feature-card">
          <h3>Forecast windows</h3>
          <p>
            Choose 7, 10, 14, or 16 days ahead so predictions match how far in advance you are scheduling—not
            only current conditions.
          </p>
        </div>
        <div className="feature-card">
          <h3>Month-aware pairs</h3>
          <p>
            Filter to a calendar month and inspect forbidden vs safe pairs with color cues, then drill into a
            single pair for full metrics and hub routing on the map.
          </p>
        </div>
        <div className="feature-card">
          <h3>Integrated risk</h3>
          <p>
            Multitask risk scores are blended with forecast disruption hints at airports A and B for the same
            horizon you selected.
          </p>
        </div>
      </section>

      <footer className="landing-footer">SHIELD · AA DFW sequence risk prototype</footer>
    </div>
  );
}
