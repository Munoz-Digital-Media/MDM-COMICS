import React from "react";
import { Database, ExternalLink } from "lucide-react";

/**
 * Data Attribution Component
 *
 * Displays attribution for data sources per licensing requirements.
 * GCD data requires CC-BY-SA 4.0 attribution.
 *
 * @param {Object} props
 * @param {Array} props.sources - Array of source names to attribute
 * @param {string} props.variant - 'inline' | 'footer' | 'detailed'
 */
export default function DataAttribution({ sources = [], variant = "footer" }) {
  // Source configurations with licensing info
  const sourceConfigs = {
    gcd: {
      name: "Grand Comics Database",
      shortName: "GCD",
      url: "https://comics.org",
      license: "CC BY-SA 4.0",
      licenseUrl: "https://creativecommons.org/licenses/by-sa/4.0/",
      color: "text-blue-400",
      required: true, // Attribution is legally required
    },
    metron: {
      name: "Metron.cloud",
      shortName: "Metron",
      url: "https://metron.cloud",
      license: "API",
      color: "text-orange-400",
      required: false,
    },
    pricecharting: {
      name: "PriceCharting",
      shortName: "PriceCharting",
      url: "https://www.pricecharting.com",
      license: "API",
      color: "text-green-400",
      required: false,
    },
  };

  // Default to GCD + Metron if no sources specified
  const activeSources = sources.length > 0 ? sources : ["gcd", "metron"];

  // Filter to only known sources
  const knownSources = activeSources.filter(s => sourceConfigs[s]);

  if (variant === "inline") {
    return (
      <span className="inline-flex items-center gap-1 text-xs text-zinc-500">
        <Database className="w-3 h-3" />
        {knownSources.map((source, i) => {
          const config = sourceConfigs[source];
          return (
            <React.Fragment key={source}>
              {i > 0 && " + "}
              <a
                href={config.url}
                target="_blank"
                rel="noopener noreferrer"
                className={`hover:underline ${config.color}`}
              >
                {config.shortName}
              </a>
            </React.Fragment>
          );
        })}
      </span>
    );
  }

  if (variant === "detailed") {
    return (
      <div className="bg-zinc-900/50 rounded-lg p-4 border border-zinc-800">
        <div className="flex items-center gap-2 mb-3">
          <Database className="w-4 h-4 text-zinc-400" />
          <span className="text-sm font-medium text-zinc-300">Data Sources</span>
        </div>
        <div className="space-y-2">
          {knownSources.map(source => {
            const config = sourceConfigs[source];
            return (
              <div key={source} className="flex items-center justify-between text-sm">
                <a
                  href={config.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={`flex items-center gap-1 hover:underline ${config.color}`}
                >
                  {config.name}
                  <ExternalLink className="w-3 h-3" />
                </a>
                {config.license && (
                  <span className="text-zinc-500 text-xs">
                    {config.licenseUrl ? (
                      <a
                        href={config.licenseUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="hover:underline"
                      >
                        {config.license}
                      </a>
                    ) : (
                      config.license
                    )}
                  </span>
                )}
              </div>
            );
          })}
        </div>
        {knownSources.includes("gcd") && (
          <p className="mt-3 text-xs text-zinc-600 leading-relaxed">
            Bibliographic data from Grand Comics Database is licensed under{" "}
            <a
              href="https://creativecommons.org/licenses/by-sa/4.0/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-400 hover:underline"
            >
              CC BY-SA 4.0
            </a>
            . Images are NOT included in this license.
          </p>
        )}
      </div>
    );
  }

  // Default: footer variant
  return (
    <p className="text-xs text-zinc-600">
      Data provided by{" "}
      {knownSources.map((source, i) => {
        const config = sourceConfigs[source];
        const isLast = i === knownSources.length - 1;
        const isSecondToLast = i === knownSources.length - 2;

        return (
          <React.Fragment key={source}>
            <a
              href={config.url}
              target="_blank"
              rel="noopener noreferrer"
              className={`hover:underline ${config.color}`}
            >
              {config.name}
            </a>
            {config.license === "CC BY-SA 4.0" && (
              <span className="text-zinc-700">
                {" "}(
                <a
                  href={config.licenseUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="hover:underline"
                >
                  {config.license}
                </a>
                )
              </span>
            )}
            {isSecondToLast && " and "}
            {!isLast && !isSecondToLast && ", "}
          </React.Fragment>
        );
      })}
    </p>
  );
}
