# Caifubao Frontend

Vue 3 + Vite + Pinia + Element Plus frontend for the Caifubao quantitative analysis platform.

## Quick Start

```bash
cd frontend
npm install
npm run dev
```

The dev server proxies `/api` requests to the backend through the Vite proxy in
`vite.config.ts` (default target `http://localhost:8000`); adjust that target
locally if your backend runs elsewhere. Real dev/prod hosts are not part of this
repository.

## Build

```bash
npm run build
```

## Lint

```bash
npm run lint
```

See `docs/DESIGN.md` for the visual design system (Linear dark theme).
