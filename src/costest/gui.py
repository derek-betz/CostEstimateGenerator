"""Simple desktop interface for running the cost estimator.

The window accepts a drag-and-dropped ``*_project_quantities.xlsx`` workbook
and forwards it to :func:`costest.cli.run`.  When the optional
``tkinterdnd2`` package is installed the drop target integrates with the host
operating system so the workbook can be dragged straight from the file
explorer.  Without it the interface falls back to a traditional "Browse" file
dialog.
"""

from __future__ import annotations

import io
import os
import queue
import re
import threading
import traceback
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:  # pragma: no cover - optional dependency
    from tkinterdnd2 import DND_FILES, TkinterDnD

    _DND_AVAILABLE = True
except Exception:  # pragma: no cover - fallback path
    TkinterDnD = None  # type: ignore[assignment]
    DND_FILES = "DND_Files"  # type: ignore[assignment]
    _DND_AVAILABLE = False

from .cli import run as run_estimator
from .project_meta import DISTRICT_CHOICES, district_to_region, normalize_district


@dataclass
class PipelineResult:
    """Container for messages communicated from the worker thread."""

    level: str
    message: str
    details: Optional[str] = None


def _split_dropped_paths(raw: str) -> List[Path]:
    """Parse Tk DND payloads into individual :class:`Path` objects."""

    if not raw:
        return []

    paths: List[str] = []
    current: List[str] = []
    brace_depth = 0

    for char in raw:
        if char == "{":
            if brace_depth == 0 and current:
                paths.append("".join(current).strip())
                current = []
            brace_depth += 1
            continue
        if char == "}":
            brace_depth = max(0, brace_depth - 1)
            if brace_depth == 0:
                paths.append("".join(current))
                current = []
            continue
        if char in ("\n", "\r"):
            continue
        if char == " " and brace_depth == 0:
            if current:
                paths.append("".join(current))
                current = []
            continue
        current.append(char)

    if current:
        paths.append("".join(current))

    return [Path(p.strip()) for p in paths if p.strip()]


class GradientFrame(tk.Canvas):
    """Canvas that renders a soft gradient with a subtle glossy highlight."""

    def __init__(
        self,
        master: tk.Misc,
        colors: List[str],
        gloss_color: Optional[str] = None,
        **kwargs: object,
    ) -> None:
        super().__init__(master, highlightthickness=0, bd=0, **kwargs)
        self._colors = colors
        self._gloss_color = gloss_color
        self.bind("<Configure>", self._draw_gradient)

    def _draw_gradient(self, _event: Optional[tk.Event] = None) -> None:
        self.delete("gradient")
        width = max(self.winfo_width(), 1)
        height = max(self.winfo_height(), 1)

        if len(self._colors) < 2:
            color = self._colors[0] if self._colors else "#000000"
            self.create_rectangle(0, 0, width, height, fill=color, outline="", tags="gradient")
        else:
            segments = len(self._colors) - 1
            step_height = height / segments
            for index in range(segments):
                start_color = self._hex_to_rgb(self._colors[index])
                end_color = self._hex_to_rgb(self._colors[index + 1])
                start_y = int(index * step_height)
                end_y = int((index + 1) * step_height) if index + 1 < segments else height
                span = max(end_y - start_y, 1)
                for offset in range(span):
                    ratio = offset / span
                    color = self._interpolate(start_color, end_color, ratio)
                    y = start_y + offset
                    self.create_line(0, y, width, y, fill=color, tags="gradient")

        if self._gloss_color and height > 6:
            gloss_height = max(int(height * 0.35), 12)
            gloss_height = min(gloss_height, height)
            self.create_rectangle(
                0,
                0,
                width,
                gloss_height,
                fill=self._gloss_color,
                outline="",
                stipple="gray25",
                tags="gradient",
            )

        self.create_line(0, height - 1, width, height - 1, fill="#000000", tags="gradient")

    @staticmethod
    def _hex_to_rgb(value: str) -> tuple[int, int, int]:
        value = value.lstrip("#")
        lv = len(value)
        step = lv // 3
        return tuple(int(value[i : i + step], 16) for i in range(0, lv, step))

    @staticmethod
    def _interpolate(start: tuple[int, int, int], end: tuple[int, int, int], ratio: float) -> str:
        clamped = max(0.0, min(1.0, ratio))
        red = int(start[0] + (end[0] - start[0]) * clamped)
        green = int(start[1] + (end[1] - start[1]) * clamped)
        blue = int(start[2] + (end[2] - start[2]) * clamped)
        return f"#{red:02x}{green:02x}{blue:02x}"


class EstimatorApp:
    """Tk-based interface for running the estimator pipeline."""

    def __init__(self) -> None:
        if _DND_AVAILABLE:
            self.root: tk.Misc = TkinterDnD.Tk()
        else:
            self.root = tk.Tk()

        self._palette: dict[str, str] = {}
        self._configure_theme()
        self.root.title("Cost Estimate Generator")
        self.root.geometry("880x680")
        self.root.minsize(720, 600)

        self._queue: "queue.Queue[PipelineResult]" = queue.Queue()
        self._worker: Optional[threading.Thread] = None
        self._current_path: Optional[Path] = None
        self._selected_path: Optional[Path] = None

        self.etcc_var = tk.StringVar()
        self.district_var = tk.StringVar()
        self.contract_filter_var = tk.StringVar(value="50")
        self._last_valid_contract_filter = 50.0
        self._drop_label_default = "Drag and drop the project quantities workbook here"
        self._district_display_strings = []
        self._district_display_to_name: dict[str, str] = {}
        for number, name in DISTRICT_CHOICES:
            display = f"{number} - {name}"
            self._district_display_strings.append(display)
            self._district_display_to_name[display] = name

        self._initial_status = "Drop a *_project_quantities.xlsx workbook to begin."
        self.status_var = tk.StringVar(value=self._initial_status)
        self._build_ui()
        self._ensure_initial_window_size()
        self.root.after(100, self._poll_queue)

    # ------------------------------------------------------------------ UI --
    def _configure_theme(self) -> None:
        try:
            self.root.tk.call("tk", "scaling", 1.2)
        except tk.TclError:
            pass

        palette = {
            "base": "#1E1E1E",
            "surface": "#252526",
            "field": "#2D2D30",
            "field_hover": "#333337",
            "field_active": "#3B3E43",
            "border": "#3C3C3C",
            "accent": "#0E639C",
            "accent_active": "#1177BB",
            "accent_pressed": "#094771",
            "accent_dim": "#1B4F72",
            "text": "#F1F1F1",
            "muted": "#C8C8C8",
            "code_bg": "#1B1D1F",
        }
        self._palette = palette

        self.root.configure(bg=palette["base"])
        default_font = "{Segoe UI} 11"
        self.root.option_add("*Font", default_font)
        self.root.option_add("*TButton.Padding", 10)
        self.root.option_add("*TEntry*Font", default_font)
        self.root.option_add("*TCombobox*Listbox.font", default_font)

        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("Background.TFrame", background=palette["base"])
        style.configure("Card.TFrame", background=palette["surface"])
        style.configure("TLabel", background=palette["surface"], foreground=palette["text"])
        style.configure("Status.TLabel", background=palette["surface"], foreground=palette["muted"])
        style.configure(
            "Heading.TLabel",
            background=palette["surface"],
            foreground=palette["text"],
            font=("Segoe UI Semibold", 18),
        )
        style.configure(
            "Subheading.TLabel",
            background=palette["surface"],
            foreground=palette["muted"],
            font=("Segoe UI", 11),
        )

        style.configure(
            "Adornment.TLabel",
            background=palette["surface"],
            foreground=palette["muted"],
            font=("Segoe UI Semibold", 12),
        )

        style.configure(
            "Filled.TEntry",
            fieldbackground=palette["field"],
            foreground=palette["text"],
            bordercolor=palette["border"],
            borderwidth=1,
            insertcolor=palette["text"],
        )
        style.map(
            "Filled.TEntry",
            fieldbackground=[("active", palette["field_hover"])],
            bordercolor=[("focus", palette["accent"])],
            foreground=[("disabled", palette["muted"])],
        )

        style.configure(
            "Filled.TCombobox",
            fieldbackground=palette["field"],
            foreground=palette["text"],
            background=palette["field"],
            bordercolor=palette["border"],
            borderwidth=1,
            arrowcolor=palette["muted"],
        )
        style.map(
            "Filled.TCombobox",
            fieldbackground=[("readonly", palette["field"]), ("hover", palette["field_hover"])],
            bordercolor=[("focus", palette["accent"])],
            foreground=[("disabled", palette["muted"])],
        )

        style.configure(
            "Accent.TButton",
            background=palette["accent"],
            foreground=palette["text"],
            borderwidth=0,
            focusthickness=1,
            focuscolor=palette["accent_active"],
        )
        style.map(
            "Accent.TButton",
            background=[
                ("disabled", palette["border"]),
                ("pressed", palette["accent_pressed"]),
                ("active", palette["accent_active"]),
            ],
            foreground=[("disabled", palette["muted"])],
        )

        style.configure(
            "Secondary.TButton",
            background=palette["field"],
            foreground=palette["text"],
            borderwidth=0,
            focusthickness=1,
            focuscolor=palette["accent"],
        )
        style.map(
            "Secondary.TButton",
            background=[
                ("disabled", palette["surface"]),
                ("pressed", palette["field_active"]),
                ("active", palette["field_hover"]),
            ],
            foreground=[("disabled", palette["muted"])],
        )

        style.configure(
            "Accent.Horizontal.TProgressbar",
            troughcolor=palette["field"],
            bordercolor=palette["field"],
            lightcolor=palette["accent_active"],
            darkcolor=palette["accent"],
            background=palette["accent"],
        )

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, style="Background.TFrame", padding=(28, 24))
        container.pack(fill=tk.BOTH, expand=True)

        card = ttk.Frame(container, style="Card.TFrame", padding=24)
        card.pack(fill=tk.BOTH, expand=True)
        card.columnconfigure(0, weight=1)

        header = GradientFrame(
            card,
            colors=["#2D2D30", "#1E1E1E"],
            gloss_color="#3A3D41",
            height=96,
        )
        header.pack(fill=tk.X, expand=False, pady=(0, 20))
        header.create_text(
            24,
            36,
            anchor="w",
            text="Cost Estimate Generator",
            fill=self._palette["text"],
            font=("Segoe UI Semibold", 22),
            tags="title",
        )
        header.create_text(
            24,
            68,
            anchor="w",
            text="Prepare polished bid-ready estimates with clarity and control.",
            fill=self._palette["muted"],
            font=("Segoe UI", 11),
            tags="subtitle",
        )

        status = ttk.Label(
            card,
            textvariable=self.status_var,
            style="Status.TLabel",
            wraplength=580,
            justify=tk.LEFT,
        )
        status.pack(fill=tk.X, pady=(0, 18))

        drop_frame = tk.Frame(
            card,
            bg=self._palette["surface"],
            highlightbackground=self._palette["accent_dim"],
            highlightcolor=self._palette["accent_dim"],
            highlightthickness=2,
            bd=0,
            height=160,
        )
        drop_frame.pack(fill=tk.X, expand=False)
        drop_frame.pack_propagate(False)

        self._drop_frame = drop_frame

        drop_label = tk.Label(
            drop_frame,
            text=self._drop_label_default,
            anchor=tk.CENTER,
            justify=tk.CENTER,
            font=("Segoe UI", 12),
            fg=self._palette["muted"],
            bg=self._palette["surface"],
            wraplength=520,
        )
        drop_label.pack(fill=tk.BOTH, expand=True, padx=18, pady=18)
        self._drop_label = drop_label

        if _DND_AVAILABLE:
            drop_frame.drop_target_register(DND_FILES)  # type: ignore[attr-defined]
            drop_frame.dnd_bind("<<Drop>>", self._handle_drop)  # type: ignore[attr-defined]
        else:  # pragma: no cover - UI only
            self._drop_label_default = "tkinterdnd2 not available. Use the Browse button below."
            drop_label.configure(text=self._drop_label_default, wraplength=520)

        self._update_drop_target(None)

        input_frame = ttk.Frame(card, style="Card.TFrame")
        input_frame.pack(fill=tk.X, pady=(24, 16))
        input_frame.columnconfigure(0, weight=1)
        input_frame.columnconfigure(1, weight=1)
        input_frame.columnconfigure(2, weight=1)

        etcc_label = ttk.Label(input_frame, text="Expected Total Contract Cost", style="Subheading.TLabel")
        etcc_label.grid(row=0, column=0, sticky=tk.W)
        etcc_field = ttk.Frame(input_frame, style="Card.TFrame")
        etcc_field.grid(row=1, column=0, sticky=tk.EW, padx=(0, 12))
        etcc_field.columnconfigure(1, weight=1)
        ttk.Label(etcc_field, text="$", style="Adornment.TLabel").grid(row=0, column=0, sticky=tk.W, padx=(0, 6))
        self.etcc_entry = ttk.Entry(etcc_field, textvariable=self.etcc_var, style="Filled.TEntry", justify=tk.RIGHT)
        self.etcc_entry.grid(row=0, column=1, sticky=tk.EW)
        self.etcc_entry.bind("<FocusIn>", self._handle_etcc_focus_in)
        self.etcc_entry.bind("<FocusOut>", self._handle_etcc_focus_out)

        district_label = ttk.Label(input_frame, text="Project District", style="Subheading.TLabel")
        district_label.grid(row=0, column=1, sticky=tk.W)
        self.district_combo = ttk.Combobox(
            input_frame,
            state="readonly",
            textvariable=self.district_var,
            values=self._district_display_strings,
            style="Filled.TCombobox",
        )
        self.district_combo.grid(row=1, column=1, sticky=tk.EW, padx=(0, 12))

        contract_filter_label = ttk.Label(input_frame, text="BidTabs Total Contract Cost Filter", style="Subheading.TLabel")
        contract_filter_label.grid(row=0, column=2, sticky=tk.W)
        contract_field = ttk.Frame(input_frame, style="Card.TFrame")
        contract_field.grid(row=1, column=2, sticky=tk.EW)
        contract_field.columnconfigure(1, weight=1)
        ttk.Label(contract_field, text="+/-", style="Adornment.TLabel").grid(row=0, column=0, sticky=tk.W, padx=(0, 6))
        self.contract_filter_entry = ttk.Entry(
            contract_field,
            textvariable=self.contract_filter_var,
            style="Filled.TEntry",
            justify=tk.RIGHT,
        )
        self.contract_filter_entry.grid(row=0, column=1, sticky=tk.EW)
        ttk.Label(contract_field, text="%", style="Adornment.TLabel").grid(row=0, column=2, sticky=tk.W, padx=(6, 0))
        self.contract_filter_entry.bind("<FocusIn>", self._handle_contract_filter_focus_in)
        self.contract_filter_entry.bind("<FocusOut>", self._handle_contract_filter_focus_out)

        # Browse button in the input frame
        self.browse_button = ttk.Button(
            input_frame,
            text="Browse for Workbook…",
            command=self._browse_file,
            style="Secondary.TButton",
        )
        self.browse_button.grid(row=2, column=0, columnspan=3, sticky=tk.EW, pady=(12, 0))

        # Button row for run and clear buttons
        button_row = ttk.Frame(card, style="Card.TFrame")
        button_row.pack(fill=tk.X, pady=(12, 12))

        self.run_button = tk.Button(
            button_row,
            text="Run Estimate",
            command=self._start_pipeline,
            state=tk.DISABLED,
            font=("Segoe UI Semibold", 12),
            bg=self._palette["accent"],
            fg=self._palette["text"],
            activebackground=self._palette["accent_active"],
            activeforeground=self._palette["text"],
            disabledforeground=self._palette["muted"],
            relief=tk.FLAT,
            bd=0,
            highlightthickness=0,
            padx=28,
            pady=18,
            cursor="hand2",
        )
        self.run_button.pack(side=tk.LEFT, fill=tk.Y)

        clear = tk.Button(
            button_row,
            text="Clear Last Result",
            command=self._clear_last_results,
            font=("Segoe UI", 12),
            bg=self._palette["field"],
            fg=self._palette["text"],
            activebackground=self._palette["field_hover"],
            activeforeground=self._palette["text"],
            disabledforeground=self._palette["muted"],
            relief=tk.FLAT,
            bd=0,
            highlightthickness=0,
            padx=24,
            pady=16,
            cursor="hand2",
        )
        clear.pack(side=tk.LEFT, padx=(12, 0), fill=tk.Y)

        self.progress = ttk.Progressbar(card, mode="indeterminate", style="Accent.Horizontal.TProgressbar")
        self.progress.pack(fill=tk.X, pady=(6, 18))

        ttk.Separator(card, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(0, 16))

        log_label = ttk.Label(card, text="Run Log", style="Subheading.TLabel")
        log_label.pack(anchor=tk.W)

        log_container = ttk.Frame(card, style="Card.TFrame")
        log_container.pack(fill=tk.BOTH, expand=True)
        log_container.columnconfigure(0, weight=1)
        log_container.rowconfigure(0, weight=1)

        self.log_widget = tk.Text(
            log_container,
            height=10,
            state=tk.DISABLED,
            wrap=tk.WORD,
            bg=self._palette["code_bg"],
            fg=self._palette["text"],
            insertbackground=self._palette["text"],
            relief=tk.FLAT,
            bd=0,
            highlightthickness=1,
            highlightbackground=self._palette["border"],
            highlightcolor=self._palette["accent"],
            font=("Consolas", 11),
        )
        self.log_widget.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(log_container, orient=tk.VERTICAL, command=self.log_widget.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_widget.configure(yscrollcommand=scrollbar.set)
        self._format_contract_filter_display(self._last_valid_contract_filter)

    def _ensure_initial_window_size(self) -> None:
        """Guarantee the window opens large enough to show the entire layout."""

        self.root.update_idletasks()
        padding = 32
        required_width = self.root.winfo_reqwidth() + padding
        required_height = self.root.winfo_reqheight() + padding
        current_width = self.root.winfo_width()
        current_height = self.root.winfo_height()

        width = max(current_width, required_width, 880)
        height = max(current_height, required_height, 680)

        self.root.minsize(width, height)
        self.root.geometry(f"{width}x{height}")

    def _show_completion_dialog(self, message: str) -> None:
        parsed = self._parse_completion_message(message)
        if parsed is None:
            messagebox.showinfo("Estimator complete", message)
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Estimator Complete")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.configure(bg=self._palette["base"])

        container = ttk.Frame(dialog, style="Background.TFrame", padding=(24, 20))
        container.pack(fill=tk.BOTH, expand=True)

        header_label = ttk.Label(container, text="Estimator Complete", style="Heading.TLabel")
        header_label.pack(anchor=tk.W, pady=(0, 12))

        body = ttk.Frame(container, style="Card.TFrame", padding=20)
        body.pack(fill=tk.BOTH, expand=True)
        body.columnconfigure(0, weight=1)

        summary = parsed["summary"]
        if summary:
            for line in summary:
                ttk.Label(body, text=line, style="TLabel", wraplength=620, justify=tk.LEFT).pack(
                    anchor=tk.W, fill=tk.X, pady=(0, 6)
                )

        headers = parsed["table_headers"]
        rows = parsed["table_rows"]
        if headers and rows:
            ttk.Label(body, text="Top Cost Drivers", style="Subheading.TLabel").pack(
                anchor=tk.W, pady=(10, 6)
            )
            table_outer = tk.Frame(body, bg=self._palette["border"], bd=1, relief=tk.SOLID)
            table_outer.pack(fill=tk.X, expand=False)
            table_frame = tk.Frame(table_outer, bg=self._palette["surface"])
            table_frame.pack(fill=tk.BOTH, expand=True)
            for column, header in enumerate(headers):
                label = tk.Label(
                    table_frame,
                    text=header,
                    font=("Segoe UI Semibold", 11),
                    bg=self._palette["accent"],
                    fg=self._palette["text"],
                    bd=1,
                    relief=tk.SOLID,
                    padx=10,
                    pady=8,
                    anchor="w",
                )
                label.grid(row=0, column=column, sticky="nsew")
                table_frame.grid_columnconfigure(column, weight=1)

            numeric_columns = {
                idx
                for idx, name in enumerate(headers)
                if name.lower() in {"quantity", "unit price est", "total cost"}
            }
            for row_index, row_values in enumerate(rows, start=1):
                for column, value in enumerate(row_values):
                    anchor = "e" if column in numeric_columns else "w"
                    label = tk.Label(
                        table_frame,
                        text=value,
                        font=("Segoe UI", 11),
                        bg=self._palette["field"] if row_index % 2 else self._palette["surface"],
                        fg=self._palette["text"],
                        bd=1,
                        relief=tk.SOLID,
                        padx=10,
                        pady=6,
                        anchor=anchor,
                        justify=tk.RIGHT if anchor == "e" else tk.LEFT,
                    )
                    label.grid(row=row_index, column=column, sticky="nsew")

        footer = parsed["footer"]
        if footer:
            ttk.Label(body, text="", style="TLabel").pack()
            for line in footer:
                ttk.Label(
                    body,
                    text=line,
                    style="TLabel",
                    wraplength=620,
                    justify=tk.LEFT,
                    foreground=self._palette["muted"],
                ).pack(anchor=tk.W, fill=tk.X, pady=(0, 4))

        other = parsed["other"]
        if other:
            ttk.Label(body, text="Additional Notes", style="Subheading.TLabel").pack(
                anchor=tk.W, pady=(12, 4)
            )
            note_box = tk.Text(
                body,
                height=min(6, len(other)),
                wrap=tk.WORD,
                bg=self._palette["code_bg"],
                fg=self._palette["text"],
                insertbackground=self._palette["text"],
                relief=tk.FLAT,
                bd=1,
                highlightthickness=1,
                highlightbackground=self._palette["border"],
                highlightcolor=self._palette["accent"],
                font=("Consolas", 11),
            )
            note_box.pack(fill=tk.BOTH, expand=False, pady=(0, 8))
            note_box.insert("1.0", "\n".join(other))
            note_box.configure(state=tk.DISABLED)

        ttk.Separator(container, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(16, 12))
        button_row = ttk.Frame(container, style="Background.TFrame")
        button_row.pack(fill=tk.X)
        button_row.columnconfigure(0, weight=1)
        ttk.Button(
            button_row,
            text="Close",
            style="Accent.TButton",
            command=dialog.destroy,
        ).grid(row=0, column=0, sticky=tk.E, padx=(0, 4))

        self._center_dialog(dialog)

    def _parse_completion_message(self, message: str) -> Optional[Dict[str, Any]]:
        if not message.strip():
            return None

        intro_lines: List[str] = []
        table_lines: List[str] = []
        footer_lines: List[str] = []
        other_lines: List[str] = []
        section = "intro"

        for raw_line in message.splitlines():
            line = raw_line.rstrip("\n")
            stripped = line.strip()
            if not stripped:
                continue

            if stripped.startswith("Top cost drivers"):
                section = "table"
                continue

            if section == "table":
                if stripped.lower().startswith("pricing"):
                    section = "footer"
                    footer_lines.append(stripped)
                else:
                    table_lines.append(stripped)
                continue

            if section == "footer":
                footer_lines.append(stripped)
                continue

            if stripped.lower().startswith("pricing"):
                footer_lines.append(stripped)
                section = "footer"
                continue

            if section == "intro":
                intro_lines.append(stripped)
            else:
                other_lines.append(stripped)

        headers, rows = self._parse_table_lines(table_lines)
        if not intro_lines and not headers and not footer_lines:
            return None

        return {
            "summary": intro_lines,
            "table_headers": headers,
            "table_rows": rows,
            "footer": footer_lines,
            "other": other_lines,
        }

    def _parse_table_lines(self, lines: List[str]) -> tuple[List[str], List[List[str]]]:
        if not lines:
            return ([], [])

        header_raw = re.split(r"\s{2,}", lines[0].strip())
        parsed_headers = [self._prettify_header(cell) for cell in header_raw if cell]
        default_headers = ["Item Code", "Description", "Quantity", "Unit Price Est", "Total Cost"]
        headers = parsed_headers if len(parsed_headers) == len(default_headers) else default_headers

        rows: List[List[str]] = []
        for raw_line in lines[1:]:
            stripped = raw_line.strip()
            if not stripped:
                continue

            parts = stripped.split(None, 1)
            if len(parts) < 2:
                continue
            code, remainder = parts

            match = re.search(r"([()\-0-9,\.]+)\s+([()\-0-9,\.]+)\s+([()\-0-9,\.]+)\s*$", remainder)
            if not match:
                fallback = [cell for cell in re.split(r"\s{2,}", stripped) if cell]
                if len(fallback) == len(headers):
                    rows.append(fallback)
                continue

            qty_str, unit_str, total_str = match.groups()
            description = remainder[: match.start()].rstrip()
            row = [
                code,
                description,
                self._format_quantity(qty_str),
                self._format_currency(unit_str),
                self._format_currency(total_str),
            ]
            rows.append(row)

        return headers, rows

    @staticmethod
    def _prettify_header(text: str) -> str:
        tokens = text.replace("_", " ").split()
        return " ".join(token.capitalize() for token in tokens)

    @staticmethod
    def _format_quantity(value: str) -> str:
        sanitized = value.replace(",", "").strip()
        try:
            numeric = float(sanitized)
        except ValueError:
            return value.strip()

        decimals = 0
        if "." in sanitized:
            decimals = min(4, len(sanitized.split(".")[1]))
        fmt = f"{{:,.{decimals}f}}" if decimals > 0 else "{:,}"
        formatted = fmt.format(numeric)
        return formatted.rstrip("0").rstrip(".") if decimals > 0 else formatted

    @staticmethod
    def _format_currency(value: str) -> str:
        sanitized = value.replace(",", "").strip()
        if sanitized.startswith("$"):
            sanitized = sanitized[1:]
        try:
            numeric = float(sanitized)
        except ValueError:
            return value.strip()
        return f"${numeric:,.2f}"

    def _center_dialog(self, window: tk.Toplevel) -> None:
        window.update_idletasks()
        root_x = self.root.winfo_rootx()
        root_y = self.root.winfo_rooty()
        width = window.winfo_width()
        height = window.winfo_height()
        x = root_x + (self.root.winfo_width() - width) // 2
        y = root_y + (self.root.winfo_height() - height) // 2
        window.geometry(f"+{max(x, 0)}+{max(y, 0)}")

    # --------------------------------------------------------------- Helpers --
    def _update_drop_target(self, selected: Optional[Path]) -> None:
        if not hasattr(self, "_drop_frame") or not hasattr(self, "_drop_label"):
            return

        if selected is None:
            bg = self._palette["surface"]
            highlight = self._palette["accent_dim"]
            text = self._drop_label_default
            fg = self._palette["muted"]
        else:
            bg = self._palette["accent_active"]
            highlight = self._palette["accent"]
            text = selected.name
            fg = self._palette["text"]

        self._drop_frame.configure(bg=bg, highlightbackground=highlight, highlightcolor=highlight)
        self._drop_label.configure(text=text, fg=fg, bg=bg)

    def _update_run_button_state(self) -> None:
        running = self._worker is not None and self._worker.is_alive()
        if running or self._selected_path is None:
            self.run_button.configure(state=tk.DISABLED)
        else:
            self.run_button.configure(state=tk.NORMAL)

    def _handle_etcc_focus_in(self, event: tk.Event) -> None:
        widget = event.widget
        self.root.after(0, lambda: widget.select_range(0, tk.END))

    def _handle_etcc_focus_out(self, _event: tk.Event) -> None:
        self._format_etcc_display()

    def _format_etcc_display(self, value: Optional[float] = None) -> None:
        if value is not None:
            numeric = float(value)
        else:
            raw = self.etcc_var.get().strip()
            if not raw:
                self.etcc_var.set("")
                return
            sanitized = raw.replace(",", "").strip()
            try:
                numeric = float(sanitized)
            except ValueError:
                return

        formatted = f"{numeric:,.2f}".rstrip("0").rstrip(".")
        self.etcc_var.set(formatted)

    def _handle_contract_filter_focus_in(self, event: tk.Event) -> None:
        widget = event.widget
        self.root.after(0, lambda: widget.select_range(0, tk.END))

    def _handle_contract_filter_focus_out(self, _event: tk.Event) -> None:
        self._format_contract_filter_display()

    def _format_contract_filter_display(self, value: Optional[float] = None) -> None:
        if value is not None and value >= 0:
            numeric = float(value)
        else:
            raw = self.contract_filter_var.get().strip()
            sanitized = raw.replace(",", "").strip()
            if not sanitized:
                numeric = self._last_valid_contract_filter or 50.0
            else:
                try:
                    numeric = abs(float(sanitized))
                except ValueError:
                    numeric = self._last_valid_contract_filter or 50.0
        if numeric <= 0:
            numeric = self._last_valid_contract_filter or 50.0
        self._last_valid_contract_filter = numeric
        formatted = f"{int(numeric)}" if numeric.is_integer() else f"{numeric:.2f}".rstrip("0").rstrip(".")
        self.contract_filter_var.set(formatted)

    def _parse_contract_filter_percent(self) -> float:
        raw = self.contract_filter_var.get().strip()
        sanitized = raw.replace(",", "").strip()
        if not sanitized:
            raise ValueError("BidTabs contract filter is required.")
        try:
            value = abs(float(sanitized))
        except ValueError as exc:  # pragma: no cover - input validation
            raise ValueError("BidTabs contract filter must be a number.") from exc
        if value <= 0:
            raise ValueError("BidTabs contract filter must be greater than zero.")
        if value > 500:
            raise ValueError("BidTabs contract filter must be less than or equal to 500%.")
        self._last_valid_contract_filter = value
        self._format_contract_filter_display(value)
        return value

    def _parse_expected_cost(self) -> float:
        raw = self.etcc_var.get().strip()
        sanitized = raw.replace(",", "").strip()
        if not sanitized:
            raise ValueError("Expected Total Contract Cost is required.")
        try:
            value = float(sanitized)
        except ValueError:
            raise ValueError("Expected Total Contract Cost must be a number.")
        if value <= 0:
            raise ValueError("Expected Total Contract Cost must be greater than zero.")
        return value

    def _resolve_project_district(self) -> tuple[str, int]:
        selection = self.district_var.get().strip()
        if not selection:
            raise ValueError("Select a project district.")
        district_name = self._district_display_to_name.get(selection) or normalize_district(selection)
        if not district_name:
            raise ValueError("Project district selection is invalid.")
        region_id = district_to_region(district_name)
        if region_id is None:
            raise ValueError("Project district selection is invalid.")
        return district_name, region_id

    def _set_running(self, running: bool) -> None:
        if running:
            self.progress.start(10)
            self.status_var.set("Running estimator…")
            self.browse_button.configure(state=tk.DISABLED)
        else:
            self.progress.stop()
            self.browse_button.configure(state=tk.NORMAL)
            if self._current_path is not None:
                self.status_var.set(f"Last run completed for {self._current_path.name}.")

    def _clear_last_results(self) -> None:
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("Estimator busy", "Please wait for the current run to finish.")
            return

        self._current_path = None
        self.status_var.set(self._initial_status)
        self.etcc_var.set("")
        self.district_var.set("")
        self.district_combo.set("")
        self._last_valid_contract_filter = 50.0
        self._format_contract_filter_display(50.0)
        self._update_drop_target(None)
        self.log_widget.configure(state=tk.NORMAL)
        self.log_widget.delete("1.0", tk.END)
        self.log_widget.configure(state=tk.DISABLED)

    def _append_log(self, text: str) -> None:
        self.log_widget.configure(state=tk.NORMAL)
        self.log_widget.insert(tk.END, text + "\n")
        self.log_widget.see(tk.END)
        self.log_widget.configure(state=tk.DISABLED)

    def _handle_drop(self, event: tk.Event) -> None:  # pragma: no cover - UI event
        paths = _split_dropped_paths(getattr(event, "data", ""))
        for path in paths:
            if path.is_file() and path.name.endswith("_project_quantities.xlsx"):
                self._select_workbook(path)
                return
        messagebox.showerror("Invalid file", "Please drop a *_project_quantities.xlsx workbook.")

    def _browse_file(self) -> None:  # pragma: no cover - UI event
        initial_dir = self._current_path.parent if self._current_path else os.getcwd()
        path = filedialog.askopenfilename(
            parent=self.root,
            title="Select project quantities workbook",
            initialdir=initial_dir,
            filetypes=[["Project quantities", "*_project_quantities.xlsx"], ["All files", "*.*"]],
        )
        if path:
            self._select_workbook(Path(path))

    # -------------------------------------------------------------- Worker --
    def _select_workbook(self, path: Path) -> None:
        if not path.name.endswith("_project_quantities.xlsx"):
            messagebox.showerror("Invalid file", "Select a *_project_quantities.xlsx workbook.")
            return

        self._selected_path = path
        self.status_var.set(f"Selected {path.name}. Fill in the inputs and click Run Estimate.")
        self._update_drop_target(path)
        self._update_run_button_state()

    def _start_pipeline(self) -> None:
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("Estimator busy", "Please wait for the current run to finish.")
            return

        path = self._selected_path
        if path is None:
            messagebox.showerror("No workbook selected", "Choose a *_project_quantities.xlsx workbook before running the estimator.")
            return

        try:
            expected_cost = self._parse_expected_cost()
            district_name, region_id = self._resolve_project_district()
            contract_filter_pct = self._parse_contract_filter_percent()
        except ValueError as exc:
            messagebox.showerror("Missing input", str(exc))
            return

        self._format_etcc_display(expected_cost)
        district_display = self.district_var.get().strip() or f"{region_id} - {district_name}"
        filter_value_display = self.contract_filter_var.get().strip()
        filter_display = f"+/-{filter_value_display}%"

        self._current_path = path
        self._append_log(f"Starting estimator for {path}…")
        self._append_log(
            f"Expected Total Contract Cost: ${expected_cost:,.2f} | Project District: {district_display} | BidTabs Contract Filter: {filter_display}"
        )
        self._set_running(True)

        self._worker = threading.Thread(
            target=self._run_pipeline,
            args=(path, expected_cost, district_name, region_id, contract_filter_pct),
            daemon=True,
        )
        self._worker.start()

    def _run_pipeline(
        self,
        path: Path,
        expected_cost: float,
        district_name: str,
        region_id: int,
        contract_filter_pct: float,
    ) -> None:
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        env_snapshot = {
            "QUANTITIES_XLSX": os.environ.get("QUANTITIES_XLSX"),
            "EXPECTED_TOTAL_CONTRACT_COST": os.environ.get("EXPECTED_TOTAL_CONTRACT_COST"),
            "PROJECT_DISTRICT": os.environ.get("PROJECT_DISTRICT"),
            "PROJECT_REGION": os.environ.get("PROJECT_REGION"),
            "BIDTABS_CONTRACT_FILTER_PCT": os.environ.get("BIDTABS_CONTRACT_FILTER_PCT"),
        }

        try:
            os.environ["QUANTITIES_XLSX"] = str(path)
            os.environ["EXPECTED_TOTAL_CONTRACT_COST"] = f"{expected_cost:.2f}"
            os.environ["PROJECT_DISTRICT"] = district_name
            os.environ["PROJECT_REGION"] = str(region_id)
            os.environ["BIDTABS_CONTRACT_FILTER_PCT"] = f"{contract_filter_pct:.6f}"
            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                exit_code = run_estimator()

            output = stdout_buffer.getvalue().strip()
            errors = stderr_buffer.getvalue().strip()

            if exit_code != 0:
                message = f"Estimator finished with exit code {exit_code}."
                details = "\n".join(filter(None, [output, errors])) or None
                self._queue.put(PipelineResult("error", message, details))
            else:
                combined = "\n".join(filter(None, [output, errors])) or "Estimator run completed successfully."
                self._queue.put(PipelineResult("info", combined))
        except Exception as exc:  # pragma: no cover - defensive
            details = "\n".join([
                stdout_buffer.getvalue().strip(),
                stderr_buffer.getvalue().strip(),
                traceback.format_exc(),
            ])
            self._queue.put(PipelineResult("error", f"Unexpected error: {exc}", details))
        finally:
            for key, previous in env_snapshot.items():
                if previous is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = previous

    # -------------------------------------------------------------- Queue --
    def _poll_queue(self) -> None:
        try:
            while True:
                item = self._queue.get_nowait()
                self._handle_queue_item(item)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._poll_queue)

    def _handle_queue_item(self, result: PipelineResult) -> None:
        self._set_running(False)
        if result.level == "info":
            self._append_log(result.message)
            self._show_completion_dialog(result.message)
        else:
            self._append_log(result.message)
            if result.details:
                self._append_log(result.details)
            messagebox.showerror("Estimator error", result.message)

    # ---------------------------------------------------------------- Main --
    def run(self) -> None:  # pragma: no cover - UI loop
        self.root.mainloop()


def main() -> None:  # pragma: no cover - entry point
    app = EstimatorApp()
    app.run()


if __name__ == "__main__":  # pragma: no cover - script mode
    main()
