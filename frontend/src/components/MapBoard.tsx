import { useEffect, useMemo } from "react";
import L from "leaflet";
import { MapContainer, TileLayer, Polyline, Marker, Popup, useMap } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { coords } from "../lib/airports";
import type { PairRow } from "../api/client";

const FORBIDDEN = "#ef4444";
const SAFE = "#22c55e";
const HUB = "#38bdf8";

const hubIcon = L.divIcon({
  className: "hub-marker",
  html: `<div style="width:22px;height:22px;border-radius:50%;background:${HUB};border:2px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,.4);"></div>`,
  iconSize: [22, 22],
  iconAnchor: [11, 11],
});

const apIcon = (label: string, highlight: boolean) =>
  L.divIcon({
    className: "ap-marker",
    html: `<div style="padding:2px 6px;border-radius:6px;font-size:11px;font-weight:700;background:${
      highlight ? "rgba(61,139,253,.95)" : "rgba(30,41,59,.9)"
    };color:#fff;border:1px solid rgba(255,255,255,.25);">${label}</div>`,
    iconSize: [36, 20],
    iconAnchor: [18, 10],
  });

function FitBounds({ bounds }: { bounds: L.LatLngBounds | null }) {
  const map = useMap();
  useEffect(() => {
    if (bounds?.isValid()) {
      const sw = bounds.getSouthWest();
      const ne = bounds.getNorthEast();
      if (sw.lat === ne.lat && sw.lng === ne.lng) {
        map.setView(sw, 5);
      } else {
        map.fitBounds(bounds, { padding: [48, 48] });
      }
    }
  }, [bounds, map]);
  return null;
}

type Props = {
  pairs: PairRow[];
  selected: PairRow | null;
};

export default function MapBoard({ pairs, selected }: Props) {
  const { chords, hubLeg, markers, bounds } = useMemo(() => {
    const chordLines: { positions: [number, number][]; color: string; key: string }[] = [];
    const b = L.latLngBounds([] as L.LatLngTuple[]);
    const ms: { code: string; pos: [number, number] }[] = [];

    const addM = (code: string) => {
      const c = coords(code);
      if (!c) return;
      const pos: [number, number] = [c[0], c[1]];
      b.extend(pos);
      if (!ms.some((m) => m.code === code)) ms.push({ code, pos });
    };

    addM("DFW");

    const selA = selected ? String(selected.airport_A) : "";
    const selB = selected ? String(selected.airport_B) : "";
    const selM = selected ? Number(selected.month) : NaN;

    for (const p of pairs) {
      const a = String(p.airport_A ?? "");
      const bb = String(p.airport_B ?? "");
      const ca = coords(a);
      const cb = coords(bb);
      if (!ca || !cb) continue;
      const forbidden = String(p.integrated_risk_class ?? "") === "Forbidden";
      const color = forbidden ? FORBIDDEN : SAFE;
      const isSel = selected && a === selA && bb === selB && Number(p.month) === selM;
      if (isSel) {
        addM(a);
        addM(bb);
        continue;
      }
      chordLines.push({
        positions: [
          [ca[0], ca[1]],
          [cb[0], cb[1]],
        ],
        color,
        key: `${a}-${bb}-${p.month}`,
      });
      addM(a);
      addM(bb);
    }

    let hubLegLine: { positions: [number, number][]; color: string } | null = null;
    if (selected) {
      const ca = coords(selA);
      const cb = coords(selB);
      const cd = coords("DFW");
      if (ca && cb && cd) {
        const positions: [number, number][] = [
          [ca[0], ca[1]],
          [cd[0], cd[1]],
          [cb[0], cb[1]],
        ];
        hubLegLine = {
          positions,
          color: String(selected.integrated_risk_class) === "Forbidden" ? FORBIDDEN : SAFE,
        };
        positions.forEach((pt) => b.extend(pt as L.LatLngTuple));
      }
    }

    return {
      chords: chordLines,
      hubLeg: hubLegLine,
      markers: ms,
      bounds: b.isValid() ? b : null,
    };
  }, [pairs, selected]);

  const center: [number, number] = bounds?.getCenter()
    ? [bounds.getCenter().lat, bounds.getCenter().lng]
    : [39.5, -98.35];

  return (
    <div className="map-wrap">
      <MapContainer center={center} zoom={4} scrollWheelZoom style={{ height: "100%", minHeight: 420 }}>
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        />
        <FitBounds bounds={bounds} />
        {chords.map((ln) => (
          <Polyline
            key={ln.key}
            positions={ln.positions}
            pathOptions={{ color: ln.color, weight: 2, opacity: 0.2 }}
          />
        ))}
        {hubLeg && (
          <Polyline
            positions={hubLeg.positions}
            pathOptions={{ color: hubLeg.color, weight: 5, opacity: 1 }}
          />
        )}
        <Marker position={coords("DFW")!} icon={hubIcon}>
          <Popup>DFW hub</Popup>
        </Marker>
        {markers
          .filter((m) => m.code !== "DFW")
          .map((m) => {
            const hi =
              !!selected &&
              (String(selected.airport_A) === m.code || String(selected.airport_B) === m.code);
            return (
              <Marker key={m.code} position={m.pos} icon={apIcon(m.code, hi)}>
                <Popup>{m.code}</Popup>
              </Marker>
            );
          })}
      </MapContainer>
    </div>
  );
}
