# Platform POC — Designsystem

## Typografi

### Font-familj
```
Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif
```
Monospace (kodvärden): `Consolas, Monaco, monospace`

### Storlekar

| Användning             | Storlek | Vikt |
|------------------------|---------|------|
| Badge / tagg           | 9px     | 700  |
| Tabell-label, chip     | 10px    | 600  |
| Brödtext, tabellrad    | 11–12px | 400  |
| Filterbar-label        | 10px    | 600  |
| Etikett, underrubrik   | 12px    | 600  |
| Rubrik (h2, kort)      | 13–14px | 700  |
| Sidrubrik              | 18px    | 700  |
| Tile-värde             | 18–22px | 700  |

### Line-height
- Tabellrader: `14px` (i 23px höga rader)
- Kompakt badge: `15px`
- Standard: browser default

---

## Färgpalett

### Primärfärger

| Namn               | Hex       | Användning                          |
|--------------------|-----------|--------------------------------------|
| Brand Blue         | `#1f64a4` | Primärknapp, aktiva states, länkar   |
| Brand Blue Hover   | `#18527f` | Hover på primärknapp                 |
| Dark Navy          | `#0d1822` | Product bar, sidebar, rubriker       |

### Neutrala

| Namn               | Hex       | Användning                          |
|--------------------|-----------|--------------------------------------|
| White              | `#fff`    | Kort, paneler, aktiva flikar         |
| Page Background    | `#e3e6e8` | Sidbakgrund (produkt-sidor)          |
| Admin Background   | `#f5f6f8` | Admin-layout bakgrund                |
| Light Grey 1       | `#f1f3f5` | Tabell-header, input-bakgrund        |
| Light Grey 2       | `#f8f9fa` | Hover-bakgrund (listor)              |
| Border             | `#e8e9ef` | Kortram (admin), tabellram           |
| Border Medium      | `#ced4da` | Input-ramar, dividers                |
| Border Light       | `#dee2e6` | Sub-toolbar-ram                      |
| Tab Inactive       | `#dee2e6` | Inaktiva rapport-flikar              |
| Tab Hover          | `#d0d4d8` | Hover på inaktiva flikar             |
| Text Primary       | `#1a1a1a` | Huvudtext                            |
| Text Dark          | `#333`    | Rubriker, kort-text                  |
| Text Secondary     | `#666`    | Sekundär text, beskrivningar         |
| Text Muted         | `#888`    | Hjälptext, etiketter, datum          |
| Text Placeholder   | `#999`    | Placeholder, ledtext                 |
| Text Light         | `#aaa`    | Svagt synlig text                    |
| Icon Default       | `#bbb`    | Ikon vilande (t.ex. pin unpinned)    |
| Icon Active        | `#555`    | Ikon aktiv (t.ex. pin pinned)        |
| Icon Hover         | `#666`    | Ikon hover                           |

### Semantiska färger

| Namn               | Hex       | Bakgrund    | Användning               |
|--------------------|-----------|-------------|--------------------------|
| Success            | `#2e7d32` | `#e8f5e9`   | Positiva tal, kopplad     |
| Error              | `#c62828` | `#ffebee`   | Negativa tal, fel         |
| Warning            | `#e65100` | `#fff3e0`   | Derive-regler, varning    |
| Info               | `#1976d2` | `#e8f4fd`   | ERP-taggar, info          |

### Diagram-färger

| Serie    | Hex       | Användning                |
|----------|-----------|---------------------------|
| Budget   | `#123B5C` | Staplar/linjer i graf     |
| Actuals  | `#59A5E6` | Staplar/linjer i graf     |

### Product-taggar (Platform Admin)

| System     | Text      | Bakgrund   |
|------------|-----------|------------|
| ERP        | `#1976d2` | `#e8f4fd`  |
| Product A  | `#4361ee` | `#eef2ff`  |
| Product B  | `#7c3aed` | `#f5f0ff`  |

---

## Spacing & Layout

### Container

| Kontext       | Max-width | Padding       |
|---------------|-----------|---------------|
| Product A     | 1480px    | `0 8px`       |
| Product B     | 1300px    | `0 8px`       |
| Platform Admin| 1100px    | `20px`        |
| Admin (i prod)| inherited | `20px`        |

### Kort (Cards)

| Kontext         | border-radius   | border              | padding | margin-bottom |
|-----------------|-----------------|----------------------|---------|---------------|
| Produktsidor    | `0 0 4px 4px`   | none                 | 18–20px | 8px           |
| Admin (i prod)  | `3px`           | `1px solid #e8e9ef`  | 18–20px | 16px          |
| Platform Admin  | `3px`           | `1px solid #e8e9ef`  | 20px    | 16px          |

### Gap-system

| Användning             | Värde |
|------------------------|-------|
| Report-tabs            | 4px   |
| Filter-grupper         | 5px   |
| Toolbar-element        | 8px   |
| Portalvy-kort          | 14px  |
| Tile grid              | 14px  |

---

## Komponenter

### Shell-bar

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Höjd              | 32px                         |
| Bakgrund          | `#fff`                       |
| Border            | `box-shadow: 0 1px 0 #e0e0e0` |
| Z-index           | 99999                        |
| Nav-länk font     | 12px / 500                   |
| Nav-länk aktiv    | `color: #1f64a4; font-weight: 600` |
| Pin-ikon          | 16×16px SVG, `#bbb` / `#555` |
| Notification badge| `#1f64a4`, `border-radius: 3px` |

### Product bar

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Höjd              | 44px                         |
| Bakgrund          | `#0d1822`                    |
| Position          | `sticky`, `top: var(--shell-height)` |
| Z-index           | 9999                         |
| Ikoner            | 20×20px SVG, `rgba(255,255,255,0.5)` |
| Ikon hover        | `rgba(255,255,255,0.7)` + bakgrund `rgba(255,255,255,0.1)` |
| Produktnamn       | 13px / 600 / `rgba(255,255,255,0.7)` |
| Divider           | `1px` bred, `rgba(255,255,255,0.12)` |

### Report-flikar (i Product bar)

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Höjd              | 37px                         |
| Min-bredd         | 120px                        |
| Font              | 12px / 500                   |
| Inaktiv bakgrund  | `#dee2e6`                    |
| Hover bakgrund    | `#d0d4d8`                    |
| Aktiv bakgrund    | `#fff`                       |
| Border-radius     | `6px 6px 0 0`               |
| Stäng-knapp       | 21×21px, `border-radius: 3px` |
| Gap mellan flikar | 4px                          |

### Sub-toolbar

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Höjd              | 36px                         |
| Bakgrund          | `#fff`                       |
| Border            | `border-bottom: 1px solid #dee2e6` |
| Ikoner            | 20×20px, `color: #555`      |
| Padding           | `0 16px`                     |

### Sidebar

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Bredd             | 260px                        |
| Bakgrund          | `#0d1822`                    |
| Position          | Fixed, vänster               |
| Z-index           | 100000                       |
| Transition        | `left 0.25s ease`            |
| Overlay           | `rgba(0,0,0,0.25)`, blur 2px |
| Nav-text          | 12px / 500 / `rgba(255,255,255,0.7)` |
| Nav aktiv         | `background: rgba(255,255,255,0.05)` |
| Section-header    | 10px / 600 / uppercase       |

### Knappar

| Typ          | Bakgrund    | Text      | Border              | Radius |
|--------------|-------------|-----------|----------------------|--------|
| Primary      | `#1f64a4`   | `#fff`    | none                 | 3px    |
| Ghost        | transparent | `#333`    | `1px solid #ced4da`  | 3px    |
| Ghost hover  | `#f0f0f0`   | `#333`    | `1px solid #ced4da`  | 3px    |
| Submit       | `#2e7d32`   | `#fff`    | none                 | 3px    |
| Danger       | `#c62828`   | `#fff`    | none                 | 3px    |

Knapp-storlek standard: padding `7px 15px`, font `12px / 600`.

### Tabell (datagrid)

| Egenskap           | Värde                        |
|--------------------|------------------------------|
| Header bakgrund    | `#f1f3f5`                    |
| Header border      | `1px solid #ced4da`          |
| Header text        | 12px / 700 / `rgba(0,0,0,0.82)` |
| Radhöjd            | 23px                         |
| Cell-padding       | `4px 12px 4px 4px`          |
| Rad-border         | `1px solid #ced4da`          |
| Numerisk kolumn    | `text-align: right; font-variant-numeric: tabular-nums` |

### Tiles (sammanfattningskort)

| Egenskap           | Värde                        |
|--------------------|------------------------------|
| Bakgrund           | `#fff`                       |
| Border             | `1px solid #e8e9ef`          |
| Border-radius      | 3px                          |
| Padding            | `14px 12px`                  |
| Titel (label)      | 10px / uppercase / `#888`    |
| Värde              | 18px / 700 / `#0d1822`      |

### Portal-kort

| Egenskap           | Värde                        |
|--------------------|------------------------------|
| Bakgrund           | `#fff`                       |
| Border-radius      | `0 0 4px 4px`               |
| Padding            | `18px`                       |
| Hover              | `box-shadow: 0 2px 8px rgba(31,100,164,0.1)` |
| Ikon               | 22–28px emoji                |
| Namn               | 13px / 700                   |
| Beskrivning        | 11px / `#888`                |

### Filter-bar (Product B)

| Egenskap           | Värde                        |
|--------------------|------------------------------|
| Bakgrund           | `#fff`                       |
| Border             | `border-bottom: 1px solid #e8e9ef` |
| Padding            | `8px 20px`                   |
| Position           | Sticky, z-index 9998         |
| Label              | 10px / 600 / uppercase / `#666` |
| Select             | `height: 24px; border: 1px solid #ced4da; border-radius: 3px` |

### Inbox-dropdown (Shell)

| Egenskap           | Värde                        |
|--------------------|------------------------------|
| Bredd              | 340px                        |
| Max-höjd           | 400px                        |
| Box-shadow         | `0 4px 20px rgba(0,0,0,0.15)` |
| Border-radius      | 3px                          |
| Uppgifts-punkt     | 6×6px cirkel, `#1f64a4`     |
| Avslutad punkt     | `#ccc`                       |

---

## Ikoner

SVG-ikoner i Tabler-stil (stroke-based, 24×24 viewBox):
- Renderas i 20×20px (product bar & sub-toolbar)
- `stroke-width: 2`, `stroke-linecap: round`, `stroke-linejoin: round`
- Färg via `currentColor`

Använda ikoner:
- ☰ Burger (sidebar toggle)
- ⭐ Star (favorit)
- 🔍 Search
- 🏠 Home
- 🔄 Refresh
- 📌 Pin (Tabler pin SVG)
- 🔔 Bell (inbox)

---

## Z-index-skikt

| Skikt              | Z-index | Komponent                  |
|--------------------|---------|----------------------------|
| Filter-bar (sticky)| 9998    | Filter-bar (Product B)     |
| Product bar        | 9999    | Product bar                |
| Sidebar overlay    | 99998   | Sidebar backdrop            |
| Shell header       | 99999   | Shell bar                  |
| Sidebar panel      | 100000  | Sidebar slide-in           |

---

## Transitions & Animationer

| Komponent          | Transition                              |
|--------------------|-----------------------------------------|
| Shell pin/unpin    | `transform 0.25s ease, opacity 0.2s ease` |
| Sidebar            | `left 0.25s ease`                       |
| Hover zone         | `height 0.2s ease, background 0.2s ease` |
| Knappar & ikoner   | `all 0.15s`                             |
| Background hover   | `background 0.15s`                      |

---

## CSS-variabler

| Variabel           | Standardvärde | Användning                   |
|--------------------|---------------|-------------------------------|
| `--shell-height`   | `32px` / `0px` | Product bar `top` position   |

---

## Hover zone (Shell unpinned)

När shell-baren är unpinned visas en tunn reveal-zon:
- Höjd: 3px → 6px vid hover
- Gradient: `linear-gradient(to right, transparent 20%, rgba(31,100,164,0.25) 50%, transparent 80%)`
- Hover-gradient: `rgba(31,100,164,0.45)`

---

## Notch Pill (Shell unpinned)

När shell-baren är unpinned visas en notch i övre högra hörnet:

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Position          | `fixed`, `top: 0`, `right: 0` |
| Höjd              | 32px                         |
| Bakgrund (yttre)  | `#0d1822` (dark)             |
| Z-index           | 100001                       |
| Content-area      | `background: #fff`, `border-radius: 0 0 10px 10px` |
| Tail              | 18px bred, `#0d1822`        |
| Inbox-knapp       | 11px, `border: 1px solid #ddd`, `border-radius: 3px` |
| Badge             | `#1f64a4`, `border-radius: 3px`, 9px / 700 |
| Pin-knapp         | 16×16px SVG, `#bbb` → `#666` hover |
| Peek-beteende     | Döljs via `classList.remove("visible")` när header har `.peek` |

---

## External Tools (Nav-länkar)

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Font              | 11px / 500, `color: #888`   |
| Hover             | `color: #555`, `background: #f0f0f0` |
| Extern-ikon (↗)   | 9×9px SVG, `opacity: 0.5`, `margin-left: 3px` |
| Dropdown-trigger  | `.shell-ext-more`: 11px, `color: #888`, inline-flex |
| Dropdown-panel    | `background: #fff`, `border-radius: 4px`, `box-shadow: 0 4px 16px rgba(0,0,0,0.15)`, `min-width: 180px` |
| Dropdown-item     | 12px / 500, `padding: 6px 14px`, `color: #555` |
| Positionering     | Dropdown wrappas i `position: relative`-container |

---

## Toast (Notification)

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Position          | `fixed`, `top: 70px`, `right: 20px` |
| Z-index           | 10000                        |
| Bakgrund          | `#fff`                       |
| Border            | `1px solid #e0e0e0`         |
| Border-left       | `3px solid #2e7d32` (success) / `3px solid #c62828` (error) |
| Border-radius     | `4px`                        |
| Box-shadow        | `0 4px 16px rgba(0,0,0,0.10)` |
| Font              | 12px / 500, `color: #333`   |
| Ikon              | 16×16px SVG, stroke `#2e7d32` (success) / `#c62828` (error) |
| Layout            | `display: flex`, `align-items: center`, `gap: 8px` |
| Animation         | `transform: translateX(120%)` → `translateX(0)`, 0.3s cubic-bezier |
| Duration          | 4s (auto-dismiss)            |

---

## Confirm Dialog (Modal)

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Overlay           | `rgba(0,0,0,0.4)`, `z-index: 100010` |
| Dialog-box        | `background: #fff`, `border-radius: 6px`, `max-width: 400px`, centrerad |
| Box-shadow        | `0 8px 32px rgba(0,0,0,0.18)` |
| Titel             | 15px / 700, `color: #0d1822`, med ⚠️-ikon |
| Body-text         | 13px, `color: #555`, `line-height: 1.6` |
| Impact-text       | 13px, `color: #555`, `border-left: 3px solid #e65100`, `background: #fff3e0` |
| Bekräfta-knapp    | `background: #c62828`, `color: #fff` (destruktiv) |
| Avbryt-knapp      | `background: #f5f5f5`, `color: #555` |

---

## Assignment Matrix Select (.pv-user-select)

| Egenskap          | Värde                        |
|-------------------|------------------------------|
| Höjd              | 26px                         |
| Font              | 12px, `color: #333`         |
| Border            | `1px solid #ced4da`, `border-radius: 3px` |
| Bakgrund          | `#fff`                       |
| Min-bredd         | 200px                        |
| Chevron           | SVG nedåtpil via `background-image`, `right: 8px` |
| Focus             | `border-color: #1f64a4`, `box-shadow: 0 0 0 2px rgba(31,100,164,0.12)` |
| Appearance        | `none` / `-webkit-appearance: none` |
