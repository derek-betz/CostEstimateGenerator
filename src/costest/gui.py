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
import ctypes
import os
import queue
import re
import threading
import traceback
import textwrap
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox, ttk

try:  # pragma: no cover - optional dependency
    from tkinterdnd2 import DND_FILES, TkinterDnD

    _DND_AVAILABLE = True
except Exception:  # pragma: no cover - fallback path
    TkinterDnD = None  # type: ignore[assignment]
    DND_FILES = "DND_Files"  # type: ignore[assignment]
    _DND_AVAILABLE = False

from .cli import run as run_estimator
from .config import load_config as load_runtime_config
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
        self.tag_lower("gradient")

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


class AdaptiveScrollableFrame(ttk.Frame):
    """Scrollable container that keeps content accessible on smaller displays."""

    def __init__(
        self,
        master: tk.Misc,
        *,
        content_style: str,
        content_padding: tuple[int, int, int, int] = (0, 0, 0, 0),
        canvas_background: Optional[str] = None,
        scrollbar_style: str = "Vertical.TScrollbar",
    ) -> None:
        super().__init__(master)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._canvas = tk.Canvas(
            self,
            highlightthickness=0,
            bd=0,
            background=canvas_background,
        )
        self._canvas.grid(row=0, column=0, sticky="nsew")

        self._scrollbar = ttk.Scrollbar(
            self,
            orient=tk.VERTICAL,
            command=self._canvas.yview,
            style=scrollbar_style,
        )
        self._scrollbar.grid(row=0, column=1, sticky="ns")
        self._canvas.configure(yscrollcommand=self._scrollbar.set)

        self.content = ttk.Frame(self._canvas, style=content_style, padding=content_padding)
        self._window_id = self._canvas.create_window((0, 0), window=self.content, anchor="nw")

        self.content.bind("<Configure>", self._update_scroll_region, add="+")
        self._canvas.bind("<Configure>", self._resize_canvas, add="+")
        self.bind("<Destroy>", self._unbind_mousewheel, add="+")

        self._mousewheel_bound = False
        self._bind_mousewheel()

    # ---------------------------------------------------------------- Private --
    def _update_scroll_region(self, _event: tk.Event) -> None:
        bbox = self._canvas.bbox("all")
        if bbox is not None:
            self._canvas.configure(scrollregion=bbox)
        self._update_scrollbar_visibility()

    def _resize_canvas(self, event: tk.Event) -> None:
        self._canvas.itemconfigure(self._window_id, width=event.width)
        self._update_scrollbar_visibility()

    def _update_scrollbar_visibility(self) -> None:
        try:
            canvas_height = self._canvas.winfo_height()
            content_height = self.content.winfo_reqheight()
        except tk.TclError:
            return
        if canvas_height <= 1:
            return
        needs_scrollbar = content_height > canvas_height + 2
        if needs_scrollbar:
            if not self._scrollbar.winfo_ismapped():
                self._scrollbar.grid()
        else:
            if self._scrollbar.winfo_ismapped():
                self._scrollbar.grid_remove()
            self._canvas.yview_moveto(0.0)

    def _bind_mousewheel(self) -> None:
        if self._mousewheel_bound:
            return
        self._canvas.bind_all("<MouseWheel>", self._handle_mousewheel, add="+")
        self._canvas.bind_all("<Button-4>", self._handle_mousewheel, add="+")
        self._canvas.bind_all("<Button-5>", self._handle_mousewheel, add="+")
        self._mousewheel_bound = True

    def _unbind_mousewheel(self, _event: Optional[tk.Event] = None) -> None:
        if not self._mousewheel_bound:
            return
        try:
            self._canvas.unbind_all("<MouseWheel>")
            self._canvas.unbind_all("<Button-4>")
            self._canvas.unbind_all("<Button-5>")
        except tk.TclError:
            pass
        self._mousewheel_bound = False

    def _handle_mousewheel(self, event: tk.Event) -> None:
        if not self.winfo_exists():
            return
        widget = self.content.winfo_containing(event.x_root, event.y_root)
        if widget is None or not self._is_descendant(widget):
            return
        if hasattr(widget, "yview") and widget.winfo_class() in {"Text", "Listbox", "Treeview"}:
            return
        delta = getattr(event, "delta", 0)
        if delta:
            steps = -int(delta / 120)
            if steps == 0:
                steps = -1 if delta > 0 else 1
            self._canvas.yview_scroll(steps, "units")
            return
        num = getattr(event, "num", None)
        if num == 4:
            self._canvas.yview_scroll(-1, "units")
        elif num == 5:
            self._canvas.yview_scroll(1, "units")

    def _is_descendant(self, widget: tk.Misc) -> bool:
        current: Optional[tk.Misc] = widget
        while current is not None:
            if current == self.content:
                return True
            current = getattr(current, "master", None)
        return False


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
        self._initialize_window_bounds()
        self._layout_mode = "wide"

        self._queue: "queue.Queue[PipelineResult]" = queue.Queue()
        self._worker: Optional[threading.Thread] = None
        self._current_path: Optional[Path] = None
        self._selected_path: Optional[Path] = None

        self.etcc_var = tk.StringVar()
        self.district_var = tk.StringVar()
        self.contract_filter_var = tk.StringVar(value="50")
        self.alt_seek_var = tk.BooleanVar(value=True)
        self._last_valid_contract_filter = 50.0
        self._snapshot_tag_var = tk.StringVar(value="Idle")
        self._snapshot_status_var = tk.StringVar(value="Waiting for workbook selection.")
        self._snapshot_workbook_var = tk.StringVar(value="No workbook selected.")
        self._snapshot_inputs_var = tk.StringVar(value="No project inputs captured yet.")
        self._snapshot_activity_var = tk.StringVar(value="Run log is empty.")
        self._snapshot_last_run_var = tk.StringVar(value="Estimator not yet run.")
        self._pipeline_started_at: Optional[datetime] = None
        self._last_run_completed_at: Optional[datetime] = None
        self._last_run_duration: Optional[timedelta] = None
        self._last_run_success: Optional[bool] = None
        self._last_run_path: Optional[Path] = None
        self._snapshot_timer_job: Optional[str] = None
        self._log_entry_count = 0
        self._last_log_message: Optional[str] = None
        self._run_log_rotation_messages: List[str] = []
        self._run_log_rotation_index = 0
        self._run_log_animation_job: Optional[str] = None
        self._tips_window: Optional[tk.Toplevel] = None
        self._explanation_window: Optional[tk.Toplevel] = None
        self._drop_label_default = "Drag and drop the project quantities workbook here"
        self._drop_hint_default = "Drag from Explorer or click to browse for it."
        self._district_display_strings = []
        self._district_display_to_name: dict[str, str] = {}
        for number, name in DISTRICT_CHOICES:
            display = f"{number} - {name}"
            self._district_display_strings.append(display)
            self._district_display_to_name[display] = name

        self.etcc_var.trace_add("write", self._on_inputs_changed)
        self.district_var.trace_add("write", self._on_inputs_changed)
        self.contract_filter_var.trace_add("write", self._on_inputs_changed)
        self.alt_seek_var.trace_add("write", self._on_inputs_changed)

        self._initial_status = "Drop a *_project_quantities.xlsx workbook to begin."
        self.status_title_var = tk.StringVar(value="Ready to Start")
        self.status_detail_var = tk.StringVar(value=self._initial_status)
        self._status_indicator: Optional[tk.Canvas] = None
        self._status_indicator_oval: Optional[int] = None
        self._drop_frame: Optional[tk.Frame] = None
        self._drop_label: Optional[tk.Label] = None
        self._drop_icon: Optional[tk.Label] = None
        self._drop_hint: Optional[tk.Label] = None
        self._drop_hover = False
        self._drop_enabled = True
        self._build_ui()
        self._refresh_workflow_snapshot()
        self._ensure_initial_window_size()
        self._ensure_window_visible()
        self.root.after(100, self._poll_queue)

    # --------------------------------------------------------------- Sizing --
    def _initialize_window_bounds(self) -> None:
        """Size the main window based on the available work area."""
        self.root.update_idletasks()
        work_left, work_top, work_right, work_bottom = self._get_work_area()
        work_width = max(work_right - work_left, 400)
        work_height = max(work_bottom - work_top, 400)

        desired_width = 1240
        desired_height = 720
        initial_width = int(min(desired_width, max(work_width * 0.94, min(960, work_width))))
        initial_height = int(min(desired_height, max(work_height * 0.9, min(620, work_height))))

        min_width = int(min(max(work_width * 0.75, 880), work_width))
        min_height = int(min(max(work_height * 0.75, 560), work_height))

        self.root.geometry(f"{initial_width}x{initial_height}")
        self.root.minsize(min_width, min_height)

    def _configure_responsive_layout(self) -> None:
        """Adapt the three column layout for compact screens."""
        self.root.bind("<Configure>", self._handle_root_resize, add="+")
        self._apply_responsive_layout(self.root.winfo_width())

    def _handle_root_resize(self, event: tk.Event) -> None:
        if event.widget is not self.root:
            return
        self._apply_responsive_layout(event.width)

    def _apply_responsive_layout(self, width: int) -> None:
        if width <= 0 or not hasattr(self, "_content_frame"):
            return

        breakpoint = 1160
        new_mode = "compact" if width < breakpoint else "wide"

        if new_mode != self._layout_mode:
            self._layout_mode = new_mode
            if new_mode == "compact":
                self._content_frame.columnconfigure(0, weight=1)
                self._content_frame.columnconfigure(1, weight=0)
                self._content_frame.rowconfigure(0, weight=0)
                self._content_frame.rowconfigure(1, weight=1)

                self._left_column.grid_configure(
                    row=0,
                    column=0,
                    columnspan=1,
                    sticky="nsew",
                    padx=0,
                    pady=(0, 16),
                    rowspan=1,
                )
                self._right_column.grid_configure(
                    row=1,
                    column=0,
                    columnspan=1,
                    sticky="nsew",
                    padx=0,
                    pady=(0, 0),
                )
                self._log_column.grid_configure(
                    row=0,
                    column=0,
                    sticky="nsew",
                    padx=0,
                    pady=(0, 16),
                )
                self._sidebar.grid_configure(
                    row=1,
                    column=0,
                    sticky="nsew",
                    padx=0,
                    pady=0,
                )
            else:
                self._content_frame.columnconfigure(0, weight=3)
                self._content_frame.columnconfigure(1, weight=4)
                self._content_frame.rowconfigure(0, weight=1)
                self._content_frame.rowconfigure(1, weight=1)

                self._left_column.grid_configure(
                    row=0,
                    column=0,
                    columnspan=1,
                    sticky="nsew",
                    padx=(0, 18),
                    pady=0,
                    rowspan=1,
                )
                self._right_column.grid_configure(
                    row=0,
                    column=1,
                    columnspan=1,
                    sticky="nsew",
                    padx=(0, 18),
                    pady=0,
                )
                self._log_column.grid_configure(row=0, column=0, sticky="nsew", padx=0, pady=(0, 18))
                self._sidebar.grid_configure(row=1, column=0, sticky="nsew", padx=0, pady=0)

        self._update_status_wrap(width)
        if hasattr(self, "_scroll_frame"):
            self._scroll_frame._update_scrollbar_visibility()  # type: ignore[attr-defined]

    def _update_status_wrap(self, width: int) -> None:
        detail_label = getattr(self, "_status_detail_label", None)
        if detail_label is not None:
            wrap = max(360, min(int(width * 0.5), 640))
            detail_label.configure(wraplength=wrap)
        hint_label = getattr(self, "_status_hint_label", None)
        if hint_label is not None:
            wrap = max(220, min(int(width * 0.22), 360))
            hint_label.configure(wraplength=wrap)

    def _get_work_area(self) -> Tuple[int, int, int, int]:
        """Return the available desktop work area (excludes taskbars when possible)."""

        if os.name == "nt" and hasattr(ctypes, "windll"):
            try:
                SPI_GETWORKAREA = 0x0030

                class RECT(ctypes.Structure):
                    _fields_ = [
                        ("left", ctypes.c_long),
                        ("top", ctypes.c_long),
                        ("right", ctypes.c_long),
                        ("bottom", ctypes.c_long),
                    ]

                rect = RECT()
                if ctypes.windll.user32.SystemParametersInfoW(SPI_GETWORKAREA, 0, ctypes.byref(rect), 0):
                    return rect.left, rect.top, rect.right, rect.bottom
            except Exception:
                pass

        return 0, 0, self.root.winfo_screenwidth(), self.root.winfo_screenheight()

    def _ensure_window_visible(self) -> None:
        """Center the main window and keep it within the visible screen area."""
        self.root.update_idletasks()
        work_left, work_top, work_right, work_bottom = self._get_work_area()
        max_width = max(work_right - work_left, 100)
        max_height = max(work_bottom - work_top, 100)

        width = max(min(self.root.winfo_width(), max_width), 1)
        height = max(min(self.root.winfo_height(), max_height), 1)

        x = work_left + max((max_width - width) // 2, 0)
        y = work_top + max((max_height - height) // 2, 0)

        if x + width > work_right:
            x = work_right - width
        if y + height > work_bottom:
            y = work_bottom - height

        x = max(x, work_left)
        y = max(y, work_top)

        self.root.geometry(f"{int(width)}x{int(height)}+{int(x)}+{int(y)}")

    # ------------------------------------------------------------------ UI --
    def _configure_theme(self) -> None:
        try:
            self.root.tk.call("tk", "scaling", 1.2)
        except tk.TclError:
            pass

        palette = {
            "base": "#0f1722",
            "surface": "#162230",
            "surface_alt": "#1d2c3d",
            "card": "#1a2838",
            "overlay": "#223447",
            "field": "#1f3347",
            "field_hover": "#243c54",
            "field_active": "#294662",
            "border": "#1f2f40",
            "outline": "#24384b",
            "accent": "#4bb3fd",
            "accent_active": "#63c5ff",
            "accent_pressed": "#2b8cd3",
            "accent_dim": "#1e4470",
            "accent_soft": "#173454",
            "success": "#3dd68c",
            "warning": "#f7b84b",
            "error": "#ff6f91",
            "text": "#f4f7fb",
            "muted": "#c4d3e0",
            "muted_alt": "#98a9bb",
            "code_bg": "#0b1119",
            "matrix_green": "#00ff41",
            "hero_start": "#1f3a55",
            "hero_end": "#0f2233",
            "hero_gloss": "#2f5170",
            "drop_idle": "#182a3a",
            "drop_hover": "#1f3c54",
            "drop_selected": "#204d6e",
        }
        self._palette = palette

        self.root.configure(bg=palette["base"])
        default_font = "{Segoe UI} 11"
        self.root.option_add("*Font", default_font)
        self.root.option_add("*TButton.Padding", 12)
        self.root.option_add("*TEntry*Font", default_font)
        self.root.option_add("*TCombobox*Listbox.font", default_font)

        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("Background.TFrame", background=palette["base"])
        style.configure("Card.TFrame", background=palette["card"])
        style.configure("CardBody.TFrame", background=palette["card"])
        style.configure("Glass.TFrame", background=palette["surface_alt"], relief=tk.FLAT)
        style.configure("Header.TFrame", background=palette["hero_start"])
        style.configure("StatusBar.TFrame", background=palette["surface_alt"])
        style.configure("Log.TFrame", background=palette["surface_alt"], relief=tk.FLAT)
        style.configure("TLabel", background=palette["card"], foreground=palette["text"])
        style.configure("Status.TLabel", background=palette["card"], foreground=palette["muted"])
        style.configure(
            "Heading.TLabel",
            background=palette["card"],
            foreground=palette["text"],
            font=("Segoe UI Semibold", 18),
        )
        style.configure(
            "Subheading.TLabel",
            background=palette["card"],
            foreground=palette["muted"],
            font=("Segoe UI", 11),
        )
        style.configure(
            "SectionHeading.TLabel",
            background=palette["card"],
            foreground=palette["text"],
            font=("Segoe UI Semibold", 14),
        )
        style.configure(
            "StatusTitle.TLabel",
            background=palette["surface_alt"],
            foreground=palette["text"],
            font=("Segoe UI Semibold", 12),
        )
        style.configure(
            "StatusDetail.TLabel",
            background=palette["surface_alt"],
            foreground=palette["muted"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "StatusHint.TLabel",
            background=palette["surface_alt"],
            foreground=palette["muted_alt"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "InstructionHeading.TLabel",
            background=palette["surface_alt"],
            foreground=palette["text"],
            font=("Segoe UI Semibold", 12),
        )
        style.configure(
            "InstructionBody.TLabel",
            background=palette["surface_alt"],
            foreground=palette["muted"],
            font=("Segoe UI", 10),
            justify=tk.LEFT,
            wraplength=260,
        )
        style.configure(
            "MetricValue.TLabel",
            background=palette["surface_alt"],
            foreground=palette["text"],
            font=("Segoe UI Semibold", 20),
        )
        style.configure(
            "MetricCaption.TLabel",
            background=palette["surface_alt"],
            foreground=palette["muted"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "Pill.TLabel",
            background=palette["accent_soft"],
            foreground=palette["accent_active"],
            font=("Segoe UI Semibold", 10),
            padding=(10, 4),
        )

        style.configure(
            "Link.TLabel",
            background=palette["card"],
            foreground=palette["accent_active"],
            font=("Segoe UI Semibold", 10, "underline"),
        )
        style.map(
            "Link.TLabel",
            foreground=[("active", palette["accent"])],
        )

        style.configure(
            "Adornment.TLabel",
            background=palette["surface_alt"],
            foreground=palette["muted"],
            font=("Segoe UI Semibold", 12),
        )

        style.configure(
            "Filled.TEntry",
            fieldbackground=palette["field"],
            foreground=palette["text"],
            bordercolor=palette["outline"],
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
            bordercolor=palette["outline"],
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
            "Toggle.TCheckbutton",
            background=palette["surface_alt"],
            foreground=palette["text"],
            focuscolor=palette["accent"],
        )
        style.map(
            "Toggle.TCheckbutton",
            foreground=[("disabled", palette["muted"])],
            background=[("active", palette["field_hover"])],
        )

        style.configure(
            "Primary.TButton",
            background=palette["accent"],
            foreground=palette["text"],
            borderwidth=0,
            focusthickness=1,
            focuscolor=palette["accent_active"],
            padding=(22, 12),
        )
        style.map(
            "Primary.TButton",
            background=[
                ("disabled", palette["accent_dim"]),
                ("pressed", palette["accent_pressed"]),
                ("active", palette["accent_active"]),
            ],
            foreground=[("disabled", palette["muted"])],
        )

        style.configure(
            "Secondary.TButton",
            background=palette["surface_alt"],
            foreground=palette["text"],
            borderwidth=0,
            focusthickness=1,
            focuscolor=palette["accent"],
            padding=(18, 10),
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
            troughcolor=palette["surface_alt"],
            bordercolor=palette["surface_alt"],
            lightcolor=palette["accent_active"],
            darkcolor=palette["accent"],
            background=palette["accent"],
            thickness=8,
        )

        style.configure(
            "Modern.Vertical.TScrollbar",
            gripcount=0,
            background=palette["surface_alt"],
            troughcolor=palette["surface"],
            bordercolor=palette["surface"],
            lightcolor=palette["surface_alt"],
            darkcolor=palette["surface_alt"],
            arrowcolor=palette["muted"],
        )
        style.map(
            "Modern.Vertical.TScrollbar",
            background=[("active", palette["field_hover"])],
            arrowcolor=[("active", palette["text"])],
        )

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, style="Background.TFrame", padding=(32, 28, 32, 24))
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        scroll_frame = AdaptiveScrollableFrame(
            container,
            content_style="Card.TFrame",
            content_padding=(0, 0, 0, 0),
            canvas_background=self._palette["base"],
            scrollbar_style="Modern.Vertical.TScrollbar",
        )
        scroll_frame.grid(row=0, column=0, sticky="nsew")
        self._scroll_frame = scroll_frame

        card = scroll_frame.content
        card.columnconfigure(0, weight=1)
        card.rowconfigure(2, weight=1)
        card.rowconfigure(3, weight=0)

        header = GradientFrame(
            card,
            colors=[self._palette["hero_start"], self._palette["hero_end"]],
            gloss_color=self._palette["hero_gloss"],
            height=190,
        )
        header.grid(row=0, column=0, sticky="ew")
        title_text = "Cost Estimate Generator"
        title_x = 40
        title_y = 74

        try:
            title_font = tkfont.Font(root=self.root, family="Impact", size=46)
        except tk.TclError:
            title_font = tkfont.Font(root=self.root, family="Segoe UI Black", size=46)
        self._title_font = title_font

        gradient_palette = [self._palette["accent_active"], "#ffffff", self._palette["accent"]]
        gradient_rgb = [GradientFrame._hex_to_rgb(color) for color in gradient_palette]
        total_chars = max(len(title_text) - 1, 1)
        current_x = title_x
        title_tag = "header_title"

        for index, char in enumerate(title_text):
            advance = title_font.measure(char)
            if char.strip():
                if len(gradient_rgb) == 1:
                    fill_color = gradient_palette[0]
                else:
                    segment_pos = (index / total_chars) * (len(gradient_rgb) - 1)
                    segment_index = int(segment_pos)
                    if segment_index >= len(gradient_rgb) - 1:
                        segment_index = len(gradient_rgb) - 2
                        local_ratio = 1.0
                    else:
                        local_ratio = segment_pos - segment_index
                    start_rgb = gradient_rgb[segment_index]
                    end_rgb = gradient_rgb[segment_index + 1]
                    fill_color = GradientFrame._interpolate(start_rgb, end_rgb, local_ratio)
                header.create_text(
                    current_x,
                    title_y,
                    anchor="w",
                    text=char,
                    fill=fill_color,
                    font=title_font,
                    tags=(title_tag,),
                )
            current_x += advance

        bbox = header.bbox(title_tag)
        if bbox:
            underline_start = bbox[0]
            underline_end = bbox[2]
            primary_line_y = bbox[3] + 6
            tagline_y = bbox[3] + 34
        else:
            underline_start = title_x
            underline_end = title_x + int(len(title_text) * 18.5)
            primary_line_y = title_y + 30
            tagline_y = title_y + 58

        header.create_line(
            underline_start,
            primary_line_y,
            underline_end,
            primary_line_y,
            fill="#00f0ff",
            width=3,
        )

        tagline_id = header.create_text(
            title_x,
            tagline_y,
            anchor="w",
            text="Prepare bid-ready estimates with clarity, accuracy, and polish.",
            fill=self._palette["muted"],
            font=("Segoe UI", 12),
        )

        header.tag_lower("gradient")
        header.tag_raise(title_tag)
        header.tag_raise(tagline_id)

        metrics_card = ttk.Frame(header, style="Glass.TFrame", padding=(18, 18))
        metrics_card.place(relx=1.0, rely=0.0, anchor="ne", x=-32, y=24)
        metrics_card.configure(width=320, height=120)
        metrics_card.pack_propagate(False)
        ttk.Label(metrics_card, text="Estimator at a Glance", style="InstructionHeading.TLabel").pack(
            anchor=tk.W, pady=(0, 6)
        )
        ttk.Label(
            metrics_card,
            text="Automated quantity analysis paired with AI-guided bid intelligence.",
            style="InstructionBody.TLabel",
            wraplength=280,
        ).pack(anchor=tk.W, fill=tk.X)

        status_bar = ttk.Frame(card, style="StatusBar.TFrame", padding=(24, 18, 24, 16))
        status_bar.grid(row=1, column=0, sticky="ew")
        status_bar.columnconfigure(1, weight=1)

        self._status_indicator = tk.Canvas(
            status_bar,
            width=18,
            height=18,
            highlightthickness=0,
            bg=self._palette["surface_alt"],
            bd=0,
        )
        self._status_indicator.grid(row=0, column=0, rowspan=2, sticky="w")
        self._status_indicator_oval = self._status_indicator.create_oval(
            2,
            2,
            16,
            16,
            fill=self._palette["success"],
            outline=self._palette["success"],
        )

        ttk.Label(status_bar, textvariable=self.status_title_var, style="StatusTitle.TLabel").grid(
            row=0, column=1, sticky="w"
        )
        status_detail_label = ttk.Label(
            status_bar,
            textvariable=self.status_detail_var,
            style="StatusDetail.TLabel",
            wraplength=640,
        )
        status_detail_label.grid(row=1, column=1, sticky="w", pady=(4, 0))
        self._status_detail_label = status_detail_label

        status_hint_label = ttk.Label(
            status_bar,
            text="All activity is recorded in the run log.",
            style="StatusHint.TLabel",
            wraplength=360,
            justify=tk.RIGHT,
        )
        status_hint_label.grid(row=0, column=2, rowspan=2, sticky="e", padx=(12, 0))
        self._status_hint_label = status_hint_label

        content = ttk.Frame(card, style="CardBody.TFrame", padding=(24, 16, 24, 24))
        content.grid(row=2, column=0, sticky="nsew")
        content.columnconfigure(0, weight=3)
        content.columnconfigure(1, weight=4)
        content.rowconfigure(0, weight=1)
        content.rowconfigure(1, weight=1)

        self._content_frame = content

        left_column = ttk.Frame(content, style="CardBody.TFrame")
        left_column.grid(row=0, column=0, sticky="nsew", padx=(0, 18))
        left_column.columnconfigure(0, weight=1)
        self._left_column = left_column

        ttk.Label(left_column, text="Project Workbook", style="SectionHeading.TLabel").grid(
            row=0, column=0, sticky="w", pady=(0, 8)
        )

        drop_frame = tk.Frame(
            left_column,
            bg=self._palette["drop_idle"],
            highlightbackground=self._palette["accent_dim"],
            highlightcolor=self._palette["accent_dim"],
            highlightthickness=2,
            bd=0,
            height=190,
            cursor="hand2",
            takefocus=1,
        )
        drop_frame.grid(row=1, column=0, sticky="ew", pady=(0, 14))
        drop_frame.grid_propagate(False)
        drop_frame.columnconfigure(0, weight=1)

        drop_icon = tk.Label(
            drop_frame,
            text="📂",
            font=("Segoe UI Emoji", 42),
            fg=self._palette["accent_active"],
            bg=self._palette["drop_idle"],
            cursor="hand2",
        )
        drop_icon.pack(pady=(24, 8))

        drop_label = tk.Label(
            drop_frame,
            text=self._drop_label_default,
            anchor=tk.CENTER,
            justify=tk.CENTER,
            font=("Segoe UI", 12),
            fg=self._palette["muted"],
            bg=self._palette["drop_idle"],
            wraplength=460,
            cursor="hand2",
        )
        drop_label.pack(fill=tk.X, padx=28)

        drop_hint = tk.Label(
            drop_frame,
            text=self._drop_hint_default,
            anchor=tk.CENTER,
            justify=tk.CENTER,
            font=("Segoe UI", 10),
            fg=self._palette["muted_alt"],
            bg=self._palette["drop_idle"],
            wraplength=420,
            cursor="hand2",
        )
        drop_hint.pack(pady=(12, 20))

        def _refresh_drop_wrap(_event: Optional[tk.Event]) -> None:
            available = max(drop_frame.winfo_width() - 56, 220)
            drop_label.configure(wraplength=available)
            drop_hint.configure(wraplength=available)

        drop_frame.bind("<Configure>", _refresh_drop_wrap, add="+")

        for widget in (drop_frame, drop_icon, drop_label, drop_hint):
            widget.bind("<Button-1>", self._handle_drop_click, add="+")
            widget.bind("<Return>", self._handle_drop_click, add="+")
            widget.bind("<space>", self._handle_drop_click, add="+")

        self._drop_frame = drop_frame
        self._drop_label = drop_label
        self._drop_icon = drop_icon
        self._drop_hint = drop_hint

        drop_frame.bind("<Enter>", lambda _event: self._set_drop_hover(True))
        drop_frame.bind("<Leave>", lambda _event: self._set_drop_hover(False))
        drop_frame.bind("<FocusIn>", lambda _event: self._set_drop_hover(True), add="+")
        drop_frame.bind("<FocusOut>", lambda _event: self._set_drop_hover(False), add="+")

        if _DND_AVAILABLE:
            drop_frame.drop_target_register(DND_FILES)  # type: ignore[attr-defined]
            drop_frame.dnd_bind("<<Drop>>", self._handle_drop)  # type: ignore[attr-defined]
            drop_frame.dnd_bind("<<DragEnter>>", lambda _event: self._set_drop_hover(True))  # type: ignore[attr-defined]
            drop_frame.dnd_bind("<<DragLeave>>", lambda _event: self._set_drop_hover(False))  # type: ignore[attr-defined]
        else:  # pragma: no cover - UI only
            self._drop_label_default = "Drag-and-drop enhancements unavailable. Click to select a workbook."
            self._drop_hint_default = "Click to locate a *_project_quantities.xlsx file."
            drop_label.configure(text=self._drop_label_default)
            drop_hint.configure(text=self._drop_hint_default)

        ttk.Label(left_column, text="Project Inputs", style="SectionHeading.TLabel").grid(
            row=2, column=0, sticky="w", pady=(24, 8)
        )

        input_frame = ttk.Frame(left_column, style="Glass.TFrame", padding=(18, 18))
        input_frame.grid(row=3, column=0, sticky="nsew")
        input_frame.columnconfigure(0, weight=1)
        input_frame.columnconfigure(1, weight=1)
        input_frame.columnconfigure(2, weight=1)

        etcc_label = ttk.Label(input_frame, text="Expected Total Contract Cost", style="Subheading.TLabel")
        etcc_label.grid(row=0, column=0, sticky=tk.W)
        etcc_field = ttk.Frame(input_frame, style="Glass.TFrame")
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

        contract_filter_label = ttk.Label(
            input_frame,
            text="BidTabs Total Contract Cost Filter",
            style="Subheading.TLabel",
        )
        contract_filter_label.grid(row=0, column=2, sticky=tk.W)
        contract_field = ttk.Frame(input_frame, style="Glass.TFrame")
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

        alt_seek_toggle = ttk.Checkbutton(
            input_frame,
            text="Enable alternate seek backfill (fills in prices when BidTabs is sparse)",
            variable=self.alt_seek_var,
            style="Toggle.TCheckbutton",
            takefocus=0,
        )
        alt_seek_toggle.grid(row=2, column=0, columnspan=3, sticky=tk.W)

        

        button_row = ttk.Frame(input_frame, style="Glass.TFrame")
        button_row.grid(row=4, column=0, columnspan=3, sticky=tk.EW, pady=(16, 0))
        button_row.columnconfigure(0, weight=1)
        button_row.columnconfigure(1, weight=1)
        button_row.columnconfigure(2, weight=1)

        self.run_button = ttk.Button(
            button_row,
            text="Run Estimate",
            command=self._start_pipeline,
            style="Primary.TButton",
            state=tk.DISABLED,
        )
        self.run_button.grid(row=0, column=0, sticky=tk.EW, padx=(0, 10))

        clear = ttk.Button(
            button_row,
            text="Clear Last Result",
            command=self._clear_last_results,
            style="Secondary.TButton",
        )
        clear.grid(row=0, column=1, sticky=tk.EW, padx=10)

        explain = ttk.Button(
            button_row,
            text="How the Estimator Works",
            command=self._show_estimator_explanations,
            style="Secondary.TButton",
        )
        explain.grid(row=0, column=2, sticky=tk.EW, padx=(10, 0))

        right_column = ttk.Frame(content, style="CardBody.TFrame")
        right_column.grid(row=0, column=1, sticky="nsew", padx=(0, 18))
        right_column.columnconfigure(0, weight=1)
        right_column.rowconfigure(0, weight=1)
        right_column.rowconfigure(1, weight=1)
        self._right_column = right_column

        log_column = ttk.Frame(right_column, style="CardBody.TFrame")
        log_column.grid(row=0, column=0, sticky="nsew", pady=(0, 18))
        log_column.columnconfigure(0, weight=1)
        log_column.rowconfigure(1, weight=1)
        self._log_column = log_column

        log_header = ttk.Frame(log_column, style="CardBody.TFrame")
        log_header.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        ttk.Label(log_header, text="Run Log", style="SectionHeading.TLabel").pack(anchor=tk.W)
        ttk.Label(
            log_header,
            text="A complete transcript of estimator activity for auditing and troubleshooting.",
            style="Status.TLabel",
        ).pack(anchor=tk.W, pady=(4, 0))

        log_container = ttk.Frame(log_column, style="Log.TFrame")
        log_container.grid(row=1, column=0, sticky="nsew")
        log_container.columnconfigure(0, weight=1)
        log_container.rowconfigure(0, weight=1)

        self.log_widget = tk.Text(
            log_container,
            height=5,
            state=tk.DISABLED,
            wrap=tk.WORD,
            bg=self._palette["code_bg"],
            fg=self._palette["matrix_green"],
            insertbackground=self._palette["matrix_green"],
            relief=tk.FLAT,
            bd=0,
            highlightthickness=0,
            padx=18,
            pady=16,
            font=("Cascadia Code", 11),
        )
        self.log_widget.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(
            log_container, orient=tk.VERTICAL, command=self.log_widget.yview, style="Modern.Vertical.TScrollbar"
        )
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_widget.configure(yscrollcommand=scrollbar.set)

        sidebar = ttk.Frame(right_column, style="CardBody.TFrame")
        sidebar.grid(row=1, column=0, sticky="nsew")
        sidebar.columnconfigure(0, weight=1)
        sidebar.rowconfigure(1, weight=1)
        self._sidebar = sidebar

        ttk.Label(sidebar, text="Workflow Snapshot", style="SectionHeading.TLabel").grid(
            row=0, column=0, sticky="w"
        )

        metrics_card = ttk.Frame(sidebar, style="Glass.TFrame", padding=(18, 18))
        metrics_card.grid(row=1, column=0, sticky="nsew", pady=(8, 16))
        metrics_card.columnconfigure(0, weight=1)
        metrics_card.rowconfigure(2, weight=1)

        ttk.Label(metrics_card, text="Workflow Snapshot", style="InstructionHeading.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(metrics_card, textvariable=self._snapshot_tag_var, style="Pill.TLabel").grid(
            row=1, column=0, sticky="w", pady=(10, 12)
        )

        metric_container = ttk.Frame(metrics_card, style="Glass.TFrame", padding=(12, 12))
        metric_container.grid(row=2, column=0, sticky="nsew")
        metric_container.columnconfigure(1, weight=1)

        snapshot_value_labels: List[ttk.Label] = []
        snapshot_rows = [
            ("Status", self._snapshot_status_var),
            ("Workbook", self._snapshot_workbook_var),
            ("Inputs", self._snapshot_inputs_var),
            ("Activity", self._snapshot_activity_var),
            ("Last run", self._snapshot_last_run_var),
        ]
        for index, (label_text, variable) in enumerate(snapshot_rows):
            pady = (0, 8 if index < len(snapshot_rows) - 1 else 0)
            ttk.Label(metric_container, text=label_text, style="MetricCaption.TLabel").grid(
                row=index, column=0, sticky="nw", pady=pady, padx=(0, 10)
            )
            value_label = ttk.Label(metric_container, textvariable=variable, style="InstructionBody.TLabel")
            value_label.grid(
                row=index, column=1, sticky="nw", pady=pady
            )
            snapshot_value_labels.append(value_label)

        snapshot_note_label = ttk.Label(
            metrics_card,
            text="Snapshot refreshes as the estimator runs and emits new log entries.",
            style="InstructionBody.TLabel",
        )
        snapshot_note_label.grid(row=3, column=0, sticky="w", pady=(12, 0))

        def _sync_snapshot_wrap(_event: Optional[tk.Event] = None) -> None:
            try:
                metric_container.update_idletasks()
                metrics_card.update_idletasks()
            except tk.TclError:
                return

            value_cell_bbox = metric_container.grid_bbox(1, 0)
            if value_cell_bbox:
                value_wrap = max(value_cell_bbox[2], 100)
            else:
                container_width = metric_container.winfo_width()
                if container_width <= 0:
                    return
                value_wrap = max(container_width - 120, 100)

            for label in snapshot_value_labels:
                label.configure(wraplength=value_wrap)

            note_width = metrics_card.winfo_width() - 36
            if note_width <= 0:
                note_width = value_wrap
            snapshot_note_label.configure(wraplength=max(note_width, 100))

        metric_container.bind("<Configure>", _sync_snapshot_wrap, add="+")
        metrics_card.bind("<Configure>", _sync_snapshot_wrap, add="+")
        self.root.after_idle(_sync_snapshot_wrap)

        footer = ttk.Frame(card, style="CardBody.TFrame", padding=(24, 0, 24, 16))
        footer.grid(row=3, column=0, sticky="ew")
        footer.columnconfigure(0, weight=1)

        tips_link = ttk.Label(footer, text="Workflow Tips", style="Link.TLabel", cursor="hand2")
        tips_link.grid(row=0, column=0, sticky="w")
        tips_link.configure(foreground=self._palette["accent_active"])
        tips_link.bind("<Button-1>", lambda _event: self._show_workflow_tips())
        tips_link.bind("<Enter>", lambda _event: tips_link.configure(foreground=self._palette["accent"]))
        tips_link.bind("<Leave>", lambda _event: tips_link.configure(foreground=self._palette["accent_active"]))

        self.log_widget.tag_configure(
            "base",
            lmargin1=12,
            lmargin2=12,
            spacing1=2,
            spacing3=4,
            foreground=self._palette["matrix_green"],
        )
        self.log_widget.tag_configure("accent", foreground=self._palette["matrix_green"])
        self.log_widget.tag_configure("success", foreground=self._palette["matrix_green"])
        self.log_widget.tag_configure("error", foreground=self._palette["matrix_green"])
        self.log_widget.tag_configure("animation", foreground=self._palette["matrix_green"])

        self._update_drop_target(None)
        self._set_status("Ready to Start", self._initial_status, "success")
        self._format_contract_filter_display(self._last_valid_contract_filter)
        self._configure_responsive_layout()

    def _show_workflow_tips(self) -> None:
        if self._tips_window and self._tips_window.winfo_exists():
            self._tips_window.deiconify()
            self._tips_window.lift()
            self._tips_window.focus_force()
            return

        tips_window = tk.Toplevel(self.root)
        tips_window.title("Workflow Tips")
        tips_window.configure(bg=self._palette["card"])
        tips_window.transient(self.root)

        def _on_close() -> None:
            self._tips_window = None
            tips_window.destroy()

        tips_window.protocol("WM_DELETE_WINDOW", _on_close)

        container = ttk.Frame(tips_window, style="CardBody.TFrame", padding=(24, 24, 24, 24))
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(0, weight=1)

        row_index = 0
        ttk.Label(container, text="Workflow Tips & Guidance", style="Heading.TLabel").grid(
            row=row_index, column=0, sticky="w"
        )
        row_index += 1

        instruction_header = ttk.Label(
            container, text="Quick start checklist", style="SectionHeading.TLabel"
        )
        instruction_header.grid(row=row_index, column=0, sticky="w", pady=(12, 8))
        row_index += 1

        checklist = [
            "Drop or click the project workbook area to load quantities.",
            "Confirm the district and Estimated Total Contract Cost selections reflect the active bid context.",
            "Use the contract filter to narrow BidTabs history if needed.",
            "Click Run Estimate and monitor the Run Log for pipeline updates.",
            "Review the Workflow Snapshot for status, inputs, and activity at a glance.",
        ]
        for index, text in enumerate(checklist, start=1):
            ttk.Label(
                container,
                text=f"{index}. {text}",
                style="InstructionBody.TLabel",
                wraplength=420,
            ).grid(row=row_index, column=0, sticky="w", pady=(0, 4))
            row_index += 1

        tips_header = ttk.Label(
            container, text="Key workflow reminders", style="SectionHeading.TLabel"
        )
        tips_header.grid(row=row_index, column=0, sticky="w", pady=(12, 8))
        row_index += 1

        tips = [
            ("📁", "Use the *_project_quantities workbook naming for instant recognition."),
            ("🎯", "Verify district and Estimated Total Contract Cost inputs to tailor the pricing intelligence."),
            ("📝", "Completion dialog surfaces top drivers and pricing commentary."),
            ("⚙️", "Re-run the estimator after updating workbook data to refresh the snapshot."),
        ]
        for icon, text in tips:
            row = ttk.Frame(container, style="Glass.TFrame", padding=(12, 12, 12, 12))
            row.grid(row=row_index, column=0, sticky="nsew", pady=(0, 10))
            row.columnconfigure(1, weight=1)
            tk.Label(
                row,
                text=icon,
                font=("Segoe UI Emoji", 18),
                fg=self._palette["accent_active"],
                bg=self._palette["surface_alt"],
            ).grid(row=0, column=0, sticky="n")
            ttk.Label(
                row,
                text=text,
                style="InstructionBody.TLabel",
                wraplength=390,
            ).grid(row=0, column=1, sticky="w", padx=(12, 0))
            row_index += 1

        close_button = ttk.Button(
            container,
            text="Close",
            style="Secondary.TButton",
            command=_on_close,
        )
        close_button.grid(row=row_index, column=0, sticky="e", pady=(18, 0))

        self._tips_window = tips_window
        tips_window.update_idletasks()
        required_width = tips_window.winfo_reqwidth() + 12
        required_height = tips_window.winfo_reqheight() + 12
        tips_window.minsize(required_width, required_height)
        tips_window.geometry(f"{required_width}x{required_height}")
        tips_window.grab_set()
        tips_window.focus_force()

    def _show_estimator_explanations(self) -> None:
        if self._explanation_window and self._explanation_window.winfo_exists():
            self._explanation_window.deiconify()
            self._explanation_window.lift()
            self._explanation_window.focus_force()
            return

        window = tk.Toplevel(self.root)
        window.title("How the Estimator Works")
        window.configure(bg=self._palette["card"])
        window.transient(self.root)

        def _on_close() -> None:
            self._explanation_window = None
            window.destroy()

        window.protocol("WM_DELETE_WINDOW", _on_close)

        container = ttk.Frame(window, style="CardBody.TFrame", padding=(24, 24, 24, 24))
        container.pack(fill=tk.BOTH, expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=1)

        ttk.Label(
            container,
            text="Understanding the Cost Estimator",
            style="Heading.TLabel",
        ).grid(row=0, column=0, sticky="w")

        button_bar = ttk.Frame(container, style="Glass.TFrame", padding=(12, 12))
        button_bar.grid(row=1, column=0, sticky="ew", pady=(20, 16))

        explanation_sections: List[tuple[str, str, str]] = [
            (
                "lehman",
                "Plain Language",
                textwrap.dedent(
                    """
                    The estimator helps you turn a quantity spreadsheet into a bid-ready cost summary.

                    1. Start by dropping the *_project_quantities workbook into the large drop area or click it to browse for the file. The app reads each pay item and quantity so you do not have to re-type them.
                    2. Choose the district and enter the Estimated Total Contract Cost range. That lets the estimator look up similar historical jobs.
                    3. Click Run Estimate. The run log shows what the tool is doing. When it finishes you will get a polished Excel report with pricing guidance.

                    In short, you load the workbook, confirm a couple of settings, and the estimator builds the pricing picture for you.
                    """
                ).strip(),
            ),
            (
                "intermediate",
                "Estimator Playbook",
                textwrap.dedent(
                    """
                    The workflow blends automated quantity parsing with curated pricing intelligence.

                    • Workbook ingestion maps pay items, units, and quantities directly from the *_project_quantities sheet.
                    • District selection routes the request to the right regional pricing curves while the Estimated Total Contract Cost filter narrows the BidTabs history to comparable jobs.
                    • During the run the pipeline enriches the items with alternate descriptions, generates AI commentary, and computes pricing bands using historical averages and machine learning adjustments.
                    • The Excel export packages everything with highlights, callouts, and an executive-ready summary so the team can review and share immediately.

                    Use this mode when you need to explain the estimator to project managers or estimators who know the basics of cost analysis.
                    """
                ).strip(),
            ),
            (
                "quantity_filter",
                "Quantity Filtering",
                textwrap.dedent(
                    """
                    Quantity comparisons begin with a +/-50% window around each pay item's project quantity so the estimator focuses on similar construction scales. If that first pass produces fewer than 10 BidTabs data points, the tool widens only the upper bound out to +100% while keeping the lower side pinned at -50%. That rerun collects larger (but still comparable) jobs without letting undersized quantities dilute the analysis. Pay items that already meet the 10-sample bar stay with the tighter band.
                    """
                ).strip(),
            ),
            (
                "technical",
                "Deep Technical Dive",
                textwrap.dedent(
                    """
                    Under the hood the GUI orchestrates the same pipeline exposed by costest.cli.run.

                    • File intake triggers the IO layer to normalize workbook structure, convert units, and validate required sheets.
                    • The pipeline stages fetch BidTabs pricing samples, apply district-specific weighting, and run alternate seek heuristics to backfill sparse items.
                    • AI summaries are generated through ai_reporter and ai_process_report modules, while stats.py calculates contract-level metrics such as weighted averages and variance envelopes.
                    • estimate_writer assembles the Excel deliverable with styling, pivot summaries, and narrative commentary.
                    • Run telemetry recorded in the Workflow Snapshot (status, inputs, and activity) mirrors the log entries emitted from each stage.

                    This view is ideal for technical stakeholders who want to understand how data flows through the estimator stack.
                    """
                ).strip(),
            ),
        ]
        for column in range(len(explanation_sections)):
            button_bar.columnconfigure(column, weight=1)

        text_container = ttk.Frame(container, style="Glass.TFrame")
        text_container.grid(row=2, column=0, sticky="nsew")
        text_container.columnconfigure(0, weight=1)
        text_container.rowconfigure(0, weight=1)

        body_text = tk.Text(
            text_container,
            wrap=tk.WORD,
            bg=self._palette["code_bg"],
            fg=self._palette["text"],
            insertbackground=self._palette["text"],
            relief=tk.FLAT,
            highlightthickness=0,
            padx=18,
            pady=16,
            font=("Segoe UI", 11),
            state=tk.DISABLED,
        )
        body_text.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(
            text_container,
            orient=tk.VERTICAL,
            command=body_text.yview,
            style="Modern.Vertical.TScrollbar",
        )
        scrollbar.grid(row=0, column=1, sticky="ns")
        body_text.configure(yscrollcommand=scrollbar.set)

        button_map: Dict[str, ttk.Button] = {}

        def _display_section(key: str) -> None:
            for section_key, button in button_map.items():
                button.configure(style="Primary.TButton" if section_key == key else "Secondary.TButton")

            for section_key, _title, content in explanation_sections:
                if section_key == key:
                    body_text.configure(state=tk.NORMAL)
                    body_text.delete("1.0", tk.END)
                    body_text.insert(tk.END, content)
                    body_text.configure(state=tk.DISABLED)
                    body_text.yview_moveto(0.0)
                    break

        section_count = len(explanation_sections)
        for column, (key, label, _content) in enumerate(explanation_sections):
            left_pad = 0 if column == 0 else 8
            right_pad = 0 if column == section_count - 1 else 8
            button = ttk.Button(
                button_bar,
                text=label,
                command=lambda section=key: _display_section(section),
                style="Secondary.TButton",
            )
            button.grid(row=0, column=column, sticky="ew", padx=(left_pad, right_pad))
            button_map[key] = button

        _display_section(explanation_sections[0][0])

        window.update_idletasks()
        required_width = max(window.winfo_reqwidth() + 12, 640)
        required_height = max(window.winfo_reqheight() + 12, 520)
        window.minsize(required_width, required_height)
        window.geometry(f"{required_width}x{required_height}")
        window.grab_set()
        window.focus_force()

        self._explanation_window = window

    def _ensure_initial_window_size(self) -> None:
        """Guarantee the window opens large enough to show the entire layout."""

        self.root.update_idletasks()
        padding = 32
        required_width = self.root.winfo_reqwidth() + padding
        required_height = self.root.winfo_reqheight() + padding

        work_left, work_top, work_right, work_bottom = self._get_work_area()
        max_width = max(work_right - work_left, 1)
        max_height = max(work_bottom - work_top, 1)

        minimum_width = min(max(required_width, 1100), max_width)
        minimum_height = min(max(required_height, 580), max_height)

        self.root.minsize(minimum_width, minimum_height)

        current_width = self.root.winfo_width()
        current_height = self.root.winfo_height()

        target_width = current_width
        target_height = current_height

        if current_width < minimum_width:
            target_width = minimum_width
        elif current_width > max_width:
            target_width = max_width

        if current_height < minimum_height:
            target_height = minimum_height
        elif current_height > max_height:
            target_height = max_height

        if target_width != current_width or target_height != current_height:
            self.root.geometry(f"{int(target_width)}x{int(target_height)}")

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

        screen_width = dialog.winfo_screenwidth()
        screen_height = dialog.winfo_screenheight()
        max_width_available = max(screen_width - 80, 320)
        max_height_available = max(screen_height - 120, 320)
        min_width = max(320, min(max(520, int(screen_width * 0.55)), max_width_available))
        min_height = max(320, min(max(440, int(screen_height * 0.5)), max_height_available))
        dialog.minsize(min_width, min_height)

        content_outer = ttk.Frame(dialog, style="Background.TFrame")
        content_outer.pack(fill=tk.BOTH, expand=True)

        scroll_container = ttk.Frame(content_outer, style="Background.TFrame")
        scroll_container.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(
            scroll_container,
            background=self._palette["base"],
            highlightthickness=0,
            bd=0,
        )
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        v_scrollbar = ttk.Scrollbar(scroll_container, orient=tk.VERTICAL, command=canvas.yview)
        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.configure(yscrollcommand=v_scrollbar.set)

        container = ttk.Frame(canvas, style="Background.TFrame", padding=(28, 24))
        canvas_window = canvas.create_window((0, 0), window=container, anchor="nw")

        def _update_scroll_region(_event: tk.Event) -> None:
            bbox = canvas.bbox("all")
            if bbox is not None:
                canvas.configure(scrollregion=bbox)

        container.bind("<Configure>", _update_scroll_region, add="+")

        def _resize_canvas(event: tk.Event) -> None:
            canvas.itemconfigure(canvas_window, width=event.width)

        canvas.bind("<Configure>", _resize_canvas, add="+")

        def _on_mousewheel(event: tk.Event) -> None:
            delta = getattr(event, "delta", 0)
            if delta:
                steps = -int(delta / 120)
                if steps == 0:
                    steps = -1 if delta > 0 else 1
                canvas.yview_scroll(steps, "units")
                return
            num = getattr(event, "num", None)
            if num == 4:
                canvas.yview_scroll(-1, "units")
            elif num == 5:
                canvas.yview_scroll(1, "units")

        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        canvas.bind_all("<Button-4>", _on_mousewheel)
        canvas.bind_all("<Button-5>", _on_mousewheel)

        def _unbind_mousewheel(_event: tk.Event) -> None:
            canvas.unbind_all("<MouseWheel>")
            canvas.unbind_all("<Button-4>")
            canvas.unbind_all("<Button-5>")

        dialog.bind("<Destroy>", _unbind_mousewheel, add="+")

        header_frame = ttk.Frame(container, style="Background.TFrame")
        header_frame.pack(fill=tk.X)
        ttk.Label(header_frame, text="Estimator Complete", style="Heading.TLabel").pack(anchor=tk.W)

        subtitle_parts: List[str] = []
        if self._last_run_path is not None:
            subtitle_parts.append(f"Workbook: {self._last_run_path.name}")
        if self._last_run_completed_at is not None:
            timestamp = self._last_run_completed_at.strftime("%b %d, %Y %I:%M %p").replace(" 0", " ")
            relative = self._format_relative_time(self._last_run_completed_at)
            subtitle_parts.append(f"Finished {timestamp} ({relative})")
        header_subtitle = " | ".join(part for part in subtitle_parts if part)
        if header_subtitle:
            ttk.Label(
                header_frame,
                text=header_subtitle,
                style="StatusDetail.TLabel",
                wraplength=720,
                justify=tk.LEFT,
            ).pack(anchor=tk.W, pady=(6, 0))

        metrics, coverage_blurb = self._derive_completion_metrics(parsed)
        if metrics:
            metrics_frame = ttk.Frame(container, style="Background.TFrame")
            metrics_frame.pack(fill=tk.X, pady=(18, 12))
            for index, metric in enumerate(metrics):
                card = ttk.Frame(metrics_frame, style="Glass.TFrame", padding=(18, 14))
                card.grid(row=0, column=index, padx=(0 if index == 0 else 14, 0), sticky="nsew")
                ttk.Label(
                    card,
                    text=metric["value"],
                    style="MetricValue.TLabel",
                    wraplength=240,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W)
                ttk.Label(
                    card,
                    text=metric["label"],
                    style="MetricCaption.TLabel",
                    wraplength=240,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, pady=(6, 0))
                detail = metric.get("detail")
                if detail:
                    ttk.Label(
                        card,
                        text=detail,
                        style="StatusDetail.TLabel",
                        wraplength=260,
                        justify=tk.LEFT,
                    ).pack(anchor=tk.W, pady=(6, 0))
                metrics_frame.columnconfigure(index, weight=1)

        methodology_line, detail_sections = self._extract_completion_sections(parsed)
        if methodology_line:
            ttk.Label(
                container,
                text=methodology_line,
                style="StatusDetail.TLabel",
                wraplength=720,
                justify=tk.LEFT,
            ).pack(fill=tk.X, pady=(0, 14))

        headers = parsed["table_headers"]
        rows = parsed["table_rows"]
        if headers and rows:
            table_card = ttk.Frame(container, style="Card.TFrame", padding=(22, 18))
            table_card.pack(fill=tk.BOTH, expand=False, pady=(0, 16))
            ttk.Label(table_card, text="Top Cost Drivers", style="SectionHeading.TLabel").pack(
                anchor=tk.W, pady=(0, 10)
            )

            table_outer = tk.Frame(table_card, bg=self._palette["border"], bd=1, relief=tk.SOLID)
            table_outer.pack(fill=tk.X, expand=False)
            table_frame = tk.Frame(table_outer, bg=self._palette["surface"])
            table_frame.pack(fill=tk.BOTH, expand=True)

            for column, header in enumerate(headers):
                header_label = tk.Label(
                    table_frame,
                    text=header,
                    font=("Segoe UI Semibold", 11),
                    bg=self._palette["accent"],
                    fg=self._palette["text"],
                    bd=1,
                    relief=tk.SOLID,
                    padx=12,
                    pady=8,
                    anchor="w",
                )
                header_label.grid(row=0, column=column, sticky="nsew")
                table_frame.grid_columnconfigure(column, weight=1)

            numeric_columns = {
                idx
                for idx, name in enumerate(headers)
                if name.lower() in {"quantity", "unit price est", "total cost"}
            }
            for row_index, row_values in enumerate(rows, start=1):
                for column, value in enumerate(row_values):
                    anchor = "e" if column in numeric_columns else "w"
                    tk.Label(
                        table_frame,
                        text=value,
                        font=("Segoe UI", 11),
                        bg=self._palette["field"] if row_index % 2 else self._palette["surface"],
                        fg=self._palette["text"],
                        bd=1,
                        relief=tk.SOLID,
                        padx=12,
                        pady=6,
                        anchor=anchor,
                        justify=tk.RIGHT if anchor == "e" else tk.LEFT,
                        wraplength=260 if column == 1 else 0,
                    ).grid(row=row_index, column=column, sticky="nsew")

            if coverage_blurb:
                ttk.Label(
                    table_card,
                    text=coverage_blurb,
                    style="StatusDetail.TLabel",
                    wraplength=720,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, pady=(12, 0))

        if detail_sections:
            details_card = ttk.Frame(container, style="Card.TFrame", padding=(22, 16))
            details_card.pack(fill=tk.BOTH, expand=False, pady=(0, 16))
            for index, section in enumerate(detail_sections):
                section_container = ttk.Frame(details_card, style="Card.TFrame")
                section_container.pack(fill=tk.X, expand=False, pady=(6 if index else 0, 10))
                ttk.Label(
                    section_container,
                    text=section["title"],
                    style="SectionHeading.TLabel",
                ).pack(anchor=tk.W, pady=(0, 6))
                for item in section["items"]:
                    row_frame = ttk.Frame(section_container, style="Card.TFrame")
                    row_frame.pack(fill=tk.X, expand=False, pady=(0, 6))
                    label_text = item.get("label", "")
                    value_text = item.get("value", "")
                    details_text = "\n".join(item.get("details", []))
                    value_column = 1 if label_text else 0
                    row_frame.columnconfigure(value_column, weight=1)

                    if label_text:
                        ttk.Label(
                            row_frame,
                            text=label_text,
                            style="StatusTitle.TLabel",
                        ).grid(row=0, column=0, sticky=tk.W)

                    ttk.Label(
                        row_frame,
                        text=value_text,
                        style="TLabel",
                        wraplength=520,
                        justify=tk.LEFT,
                    ).grid(
                        row=0,
                        column=value_column,
                        sticky=tk.W,
                        padx=(12 if label_text else 0, 0),
                    )

                    if details_text:
                        ttk.Label(
                            row_frame,
                            text=details_text,
                            style="StatusDetail.TLabel",
                            wraplength=520,
                            justify=tk.LEFT,
                        ).grid(
                            row=1,
                            column=value_column,
                            sticky=tk.W,
                            padx=(12 if label_text else 0, 0),
                            pady=(2, 0),
                        )

        other = parsed["other"]
        filtered_other: List[str] = []
        skipping_summary_block = False
        for line in other or []:
            lowered = line.lower()
            if not skipping_summary_block and lowered.startswith("summary"):
                skipping_summary_block = True
                continue
            if skipping_summary_block:
                if "=== summary ===" in lowered:
                    skipping_summary_block = False
                continue
            filtered_other.append(line)

        if filtered_other:
            notes_card = ttk.Frame(container, style="Glass.TFrame", padding=(18, 14))
            notes_card.pack(fill=tk.BOTH, expand=False, pady=(0, 16))
            ttk.Label(
                notes_card,
                text="Additional Notes",
                style="SectionHeading.TLabel",
            ).pack(anchor=tk.W, pady=(0, 6))
            for line in filtered_other:
                ttk.Label(
                    notes_card,
                    text=line,
                    style="StatusDetail.TLabel",
                    wraplength=720,
                    justify=tk.LEFT,
                ).pack(anchor=tk.W, pady=(0, 4))

        footer = ttk.Frame(dialog, style="Background.TFrame", padding=(28, 0, 28, 24))
        footer.pack(fill=tk.X)
        ttk.Separator(footer, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(0, 12))
        button_row = ttk.Frame(footer, style="Background.TFrame")
        button_row.pack(fill=tk.X)
        button_row.columnconfigure(0, weight=1)
        ttk.Button(
            button_row,
            text="Close",
            style="Accent.TButton",
            command=dialog.destroy,
        ).grid(row=0, column=0, sticky=tk.E)

        self._center_dialog(dialog)

    def _parse_completion_message(self, message: str) -> Optional[Dict[str, Any]]:
        if not message.strip():
            return None

        intro_lines: List[str] = []
        table_lines: List[str] = []
        footer_lines: List[str] = []
        footer_lines_raw: List[str] = []
        other_lines: List[str] = []
        section = "intro"

        for raw_line in message.splitlines():
            line = raw_line.rstrip("\n")
            stripped = line.strip()
            if not stripped:
                continue

            lowered = stripped.lower()

            if section == "table":
                if lowered.startswith("pricing"):
                    section = "footer"
                    footer_lines.append(stripped)
                    footer_lines_raw.append(line)
                else:
                    table_lines.append(line)
                continue

            if lowered.startswith("pricing"):
                footer_lines.append(stripped)
                footer_lines_raw.append(line)
                section = "footer"
                continue

            if "top" in lowered and "cost" in lowered and "driver" in lowered:
                section = "table"
                continue

            if section == "footer":
                footer_lines.append(stripped)
                footer_lines_raw.append(line)
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
            "footer_full": footer_lines_raw,
            "other": other_lines,
        }

    def _derive_completion_metrics(
        self, parsed: Dict[str, Any]
    ) -> Tuple[List[Dict[str, str]], Optional[str]]:
        summary_lines = parsed.get("summary") or []
        subtotal_value: Optional[float] = None
        subtotal_display: Optional[str] = None

        subtotal_line = next(
            (line for line in summary_lines if "project subtotal" in line.lower()), None
        )
        if subtotal_line:
            match = re.search(r"\$[0-9,]+(?:\.[0-9]+)?", subtotal_line)
            if match:
                subtotal_display = match.group().strip()
                subtotal_value = self._parse_currency_amount(subtotal_display)

        if subtotal_value is None:
            for line in summary_lines:
                lowered = line.lower()
                if "contract cost" in lowered or "expected" in lowered:
                    continue
                match = re.search(r"\$[0-9,]+(?:\.[0-9]+)?", line)
                if match:
                    subtotal_display = match.group().strip()
                    subtotal_value = self._parse_currency_amount(subtotal_display)
                    if subtotal_value is not None:
                        break

        if subtotal_value is None:
            additional_lines: List[str] = []
            additional_lines.extend(parsed.get("footer_full") or [])
            additional_lines.extend(parsed.get("footer") or [])
            additional_lines.extend(parsed.get("other") or [])
            for line in additional_lines:
                lowered = line.lower()
                if "project subtotal" not in lowered:
                    continue
                match = re.search(r"\$[0-9,]+(?:\.[0-9]+)?", line)
                if match:
                    subtotal_display = match.group().strip()
                    subtotal_value = self._parse_currency_amount(subtotal_display)
                    if subtotal_value is not None:
                        break

        rows: List[List[str]] = parsed.get("table_rows") or []
        top_total_value: Optional[float] = None
        if rows:
            totals: List[float] = []
            for row_values in rows:
                if not row_values:
                    continue
                total_raw = row_values[-1]
                parsed_total = self._parse_currency_amount(total_raw)
                if parsed_total is not None:
                    totals.append(parsed_total)
            if totals:
                top_total_value = sum(totals)

        metrics: List[Dict[str, str]] = []
        if subtotal_value is not None:
            metrics.append({
                "label": "Project Subtotal",
                "value": subtotal_display or self._format_currency(f"{subtotal_value:.2f}"),
                "detail": "Sum of estimated pay items",
            })

        coverage_blurb: Optional[str] = None
        percent_text: Optional[str] = None
        if (
            top_total_value is not None
            and subtotal_value is not None
            and subtotal_value > 0
        ):
            percentage = max(min(top_total_value / subtotal_value, 1.0), 0.0)
            percent_text = f"{percentage:.0%}" if percentage >= 0.1 else f"{percentage:.1%}"
            coverage_blurb = (
                f"Top {len(rows)} cost drivers account for {percent_text} of the subtotal."
            )

        if top_total_value is not None:
            top_detail_parts: List[str] = []
            if rows:
                top_detail_parts.append(f"{len(rows)} items")
            if percent_text:
                top_detail_parts.append(f"{percent_text} of subtotal")
            top_metric = {
                "label": "Top Drivers",
                "value": self._format_currency(f"{top_total_value:.2f}"),
            }
            detail_text = " | ".join(top_detail_parts)
            if detail_text:
                top_metric["detail"] = detail_text
            metrics.append(top_metric)

        duration_value = ""
        if self._last_run_duration is not None:
            duration_value = self._format_duration(self._last_run_duration)

        if duration_value or self._last_run_completed_at is not None:
            duration_metric = {
                "label": "Run Duration",
                "value": duration_value or "Not captured",
            }
            if self._last_run_completed_at is not None:
                duration_metric["detail"] = f"Completed {self._format_relative_time(self._last_run_completed_at)}"
            metrics.append(duration_metric)

    # Extract count for alternates from CLI summary line
        try:
            all_lines: List[str] = []
            all_lines.extend(summary_lines)
            all_lines.extend(parsed.get("footer_full") or [])
            all_lines.extend(parsed.get("footer") or [])
            all_lines.extend(parsed.get("other") or [])
            import re as _re
            count_line = next((ln for ln in all_lines if "alternates used" in ln.lower()), None)
            if count_line:
                m_alt = _re.search(r"Alternates used:\s*(\d+)", count_line, _re.IGNORECASE)
                if m_alt:
                    metrics.append({
                        "label": "Alternates Used",
                        "value": m_alt.group(1),
                        "detail": "Geometry-based backfill",
                    })
                # Section surrogate metric removed
        except Exception:
            pass

        return metrics, coverage_blurb

    def _extract_completion_sections(
        self, parsed: Dict[str, Any]
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        raw_lines: List[str] = parsed.get("footer_full") or parsed.get("footer") or []
        if not raw_lines:
            return None, []

        lines = [line for line in raw_lines if line.strip()]
        methodology_line: Optional[str] = None
        if lines and lines[0].strip().lower().startswith("pricing"):
            methodology_line = lines.pop(0).strip()

        sections: List[Dict[str, Any]] = []
        current_section: Optional[Dict[str, Any]] = None
        last_item: Optional[Dict[str, Any]] = None

        for raw_line in lines:
            stripped = raw_line.strip()
            if not stripped:
                continue
            indent = len(raw_line) - len(raw_line.lstrip(" "))
            if stripped.endswith(":") and indent == 0:
                current_section = {"title": stripped[:-1].strip(), "items": []}
                sections.append(current_section)
                last_item = None
                continue
            if current_section is None:
                current_section = {"title": "Details", "items": []}
                sections.append(current_section)
            is_bullet = stripped.startswith("- ")
            content = stripped[2:].strip() if is_bullet else stripped
            if is_bullet:
                label, _, value = content.partition(":")
                if value:
                    item = {"label": label.strip(), "value": value.strip(), "details": []}
                else:
                    item = {"label": "", "value": content, "details": []}
                current_section["items"].append(item)
                last_item = item
                continue
            if indent > 0 and last_item is not None:
                last_item.setdefault("details", []).append(content)
                continue
            label, _, value = content.partition(":")
            if value:
                item = {"label": label.strip(), "value": value.strip(), "details": []}
            else:
                item = {"label": "", "value": content, "details": []}
            current_section["items"].append(item)
            last_item = item

        filtered_sections = [section for section in sections if section["items"]]
        return methodology_line, filtered_sections

    def _parse_table_lines(self, lines: List[str]) -> tuple[List[str], List[List[str]]]:
        if not lines:
            return ([], [])

        expected_columns = ["ITEM_CODE", "DESCRIPTION", "QUANTITY", "UNIT_PRICE_EST", "TOTAL_COST"]
        try:  # Prefer a robust fixed-width parse when pandas is available.
            import pandas as pd  # type: ignore import

            table_text = "\n".join(lines)
            table_df = pd.read_fwf(io.StringIO(table_text), dtype=str)  # type: ignore[arg-type]
        except Exception:  # pragma: no cover - pandas missing or parsing failed
            table_df = None  # type: ignore[assignment]

        if table_df is not None and not table_df.empty:
            if all(column in table_df.columns for column in expected_columns):
                headers = [self._prettify_header(column) for column in expected_columns]
                rows: List[List[str]] = []
                for _, row in table_df[expected_columns].fillna("").iterrows():
                    code = str(row["ITEM_CODE"]).strip()
                    if not code:
                        continue
                    description = str(row["DESCRIPTION"]).strip()
                    quantity = self._format_quantity(str(row["QUANTITY"]))
                    unit_price = self._format_currency(str(row["UNIT_PRICE_EST"]))
                    total_cost = self._format_currency(str(row["TOTAL_COST"]))
                    rows.append([code, description, quantity, unit_price, total_cost])
                if rows:
                    return headers, rows

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

    @staticmethod
    def _parse_currency_amount(text: str) -> Optional[float]:
        sanitized = text.replace("$", "").replace(",", "").strip()
        sanitized = sanitized.rstrip(".")
        if not sanitized:
            return None
        try:
            return float(sanitized)
        except ValueError:
            return None

    @staticmethod
    def _format_duration(duration: Optional[timedelta]) -> str:
        if duration is None:
            return ""
        total_seconds = int(duration.total_seconds())
        if total_seconds <= 0:
            return "0s"
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        parts: List[str] = []
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        elif hours and seconds:
            parts.append(f"{seconds}s")
        if not hours:
            parts.append(f"{seconds}s")
        return " ".join(dict.fromkeys(parts)) or "0s"

    @staticmethod
    def _format_relative_time(moment: datetime) -> str:
        delta = datetime.now() - moment
        seconds = int(delta.total_seconds())
        if seconds < 5:
            return "just now"
        if seconds < 60:
            return f"{seconds}s ago"
        minutes, seconds = divmod(seconds, 60)
        if minutes < 60:
            return f"{minutes}m ago"
        hours, minutes = divmod(minutes, 60)
        if hours < 24:
            return f"{hours}h ago"
        days, hours = divmod(hours, 24)
        if days < 7:
            return f"{days}d ago"
        weeks, days = divmod(days, 7)
        if weeks < 4:
            return f"{weeks}w ago"
        months = weeks // 4
        if months < 12:
            return f"{months}mo ago"
        years = months // 12
        return f"{years}y ago"

    @staticmethod
    def _format_path_for_display(path: Path, max_name: int = 42, max_parent: int = 48) -> str:
        name = path.name
        if len(name) > max_name:
            name = name[: max_name - 1] + "…"
        parent = str(path.parent)
        if parent in (".", ""):
            return name
        if len(parent) > max_parent:
            parent = "…" + parent[-(max_parent - 1) :]
        return f"{name}\n{parent}"

    def _center_dialog(self, window: tk.Toplevel) -> None:
        window.update_idletasks()
        screen_width = window.winfo_screenwidth()
        screen_height = window.winfo_screenheight()
        width = window.winfo_width()
        height = window.winfo_height()

        max_width = max(320, min(width, int(screen_width * 0.9)))
        max_height = max(280, min(height, int(screen_height * 0.85)))
        if width != max_width or height != max_height:
            window.geometry(f"{max_width}x{max_height}")
            window.update_idletasks()
            width = max_width
            height = max_height

        x = max((screen_width - width) // 2, 0)
        y = max((screen_height - height) // 2, 0)

        window.geometry(f"{width}x{height}+{x}+{y}")

    # --------------------------------------------------------------- Helpers --
    def _set_status(
        self,
        title: str,
        detail: Optional[str] = None,
        indicator: Optional[str] = None,
    ) -> None:
        self.status_title_var.set(title)
        if detail is not None:
            self.status_detail_var.set(detail)
        if indicator:
            color = self._palette.get(indicator, indicator)
            self._update_status_indicator(color)

    def _update_status_indicator(self, color: str) -> None:
        if self._status_indicator is not None and self._status_indicator_oval is not None:
            self._status_indicator.itemconfigure(self._status_indicator_oval, fill=color, outline=color)

    def _set_drop_hover(self, active: bool) -> None:
        self._drop_hover = active
        if self._selected_path is None:
            self._update_drop_target(None)

    def _set_drop_enabled(self, enabled: bool) -> None:
        self._drop_enabled = enabled
        cursor = "arrow" if not enabled else "hand2"
        frame = self._drop_frame
        for widget in (frame, self._drop_icon, self._drop_label, self._drop_hint):
            if widget is None:
                continue
            try:
                widget.configure(cursor=cursor)
            except tk.TclError:
                continue
        if frame is not None:
            frame.configure(takefocus=0 if not enabled else 1)

    def _update_drop_target(self, selected: Optional[Path]) -> None:
        if self._drop_frame is None or self._drop_label is None:
            return

        if selected is None:
            bg = self._palette["drop_hover"] if self._drop_hover else self._palette["drop_idle"]
            highlight = self._palette["accent_active"] if self._drop_hover else self._palette["accent_dim"]
            text = self._drop_label_default
            fg = self._palette["muted"]
            icon = "📂"
            hint = self._drop_hint_default
            hint_fg = self._palette["muted_alt"]
        else:
            bg = self._palette["drop_selected"]
            highlight = self._palette["accent"]
            text = selected.name
            fg = self._palette["text"]
            icon = "🗂️"
            hint = "Inputs locked. Review values then run the estimator."
            hint_fg = self._palette["muted"]

        self._drop_frame.configure(bg=bg, highlightbackground=highlight, highlightcolor=highlight)
        self._drop_label.configure(text=text, fg=fg, bg=bg)
        if self._drop_icon is not None:
            self._drop_icon.configure(text=icon, bg=bg, fg=self._palette["accent_active"])
        if self._drop_hint is not None:
            self._drop_hint.configure(text=hint, fg=hint_fg, bg=bg)

    def _update_run_button_state(self) -> None:
        running = self._worker is not None and self._worker.is_alive()
        if running or self._selected_path is None:
            self.run_button.configure(state=tk.DISABLED)
        else:
            self.run_button.configure(state=tk.NORMAL)

    def _on_inputs_changed(self, *_: object) -> None:
        self._refresh_workflow_snapshot()

    def _stop_snapshot_timer(self) -> None:
        if self._snapshot_timer_job is not None:
            try:
                self.root.after_cancel(self._snapshot_timer_job)
            except tk.TclError:
                pass
            self._snapshot_timer_job = None

    def _refresh_workflow_snapshot(self) -> None:
        self._stop_snapshot_timer()

        now = datetime.now()
        running = self._worker is not None and self._worker.is_alive()

        if running:
            tag = "Live run"
            if self._pipeline_started_at:
                elapsed = now - self._pipeline_started_at
                status_text = f"Estimator running | {self._format_duration(elapsed)} elapsed"
            else:
                status_text = "Estimator running…"
        elif self._last_run_completed_at:
            tag = "Success" if self._last_run_success else "Attention"
            duration_text = self._format_duration(self._last_run_duration)
            relative_text = self._format_relative_time(self._last_run_completed_at)
            outcome = "Last run succeeded" if self._last_run_success else "Last run failed"
            detail = " | ".join(bit for bit in (duration_text, relative_text) if bit)
            status_text = f"{outcome}{f' ({detail})' if detail else ''}"
        elif self._selected_path:
            tag = "Ready"
            status_text = "Workbook staged. Confirm inputs then run."
        else:
            tag = "Idle"
            status_text = "Waiting for workbook selection."

        self._snapshot_tag_var.set(tag)
        self._snapshot_status_var.set(status_text)

        workbook_sections: List[str] = []
        if self._selected_path:
            workbook_sections.append(f"Selected - {self._format_path_for_display(self._selected_path)}")
        if self._last_run_path and (self._selected_path is None or self._last_run_path != self._selected_path):
            workbook_sections.append(f"Last run - {self._format_path_for_display(self._last_run_path)}")
        if not workbook_sections:
            workbook_sections.append("No workbook selected.")
        self._snapshot_workbook_var.set("\n\n".join(workbook_sections))

        etcc = self.etcc_var.get().strip()
        district = self.district_var.get().strip()
        filter_value = self.contract_filter_var.get().strip()
        input_parts = []
        if etcc:
            input_parts.append(f"ETCC ${etcc}")
        if district:
            input_parts.append(f"District {district}")
        if filter_value:
            display_value = filter_value.rstrip("%").strip()
            if not display_value.startswith("+/-"):
                display_value = f"+/-{display_value}"
            filter_display = f"{display_value}%"
            input_parts.append(f"Filter {filter_display}")
        self._snapshot_inputs_var.set(" | ".join(input_parts) if input_parts else "No project inputs captured yet.")

        if self._log_entry_count == 0:
            activity_text = "Run log is empty."
        else:
            last_line = (self._last_log_message or "").strip().splitlines()
            last_summary = last_line[-1] if last_line else ""
            if last_summary and len(last_summary) > 80:
                last_summary = last_summary[:77] + "…"
            suffix = f" | Last: {last_summary}" if last_summary else ""
            activity_text = f"{self._log_entry_count} log entries{suffix}"
        self._snapshot_activity_var.set(activity_text)

        if self._last_run_completed_at:
            outcome = "Success" if self._last_run_success else "Failed"
            duration_text = self._format_duration(self._last_run_duration)
            relative_text = self._format_relative_time(self._last_run_completed_at)
            header_bits = [outcome]
            if duration_text:
                header_bits.append(duration_text)
            if relative_text:
                header_bits.append(relative_text)
            timestamp = self._last_run_completed_at.strftime("%b %d at %I:%M %p").replace(" 0", " ")
            lines = [" | ".join(header_bits), timestamp]
            if self._last_run_path:
                lines.append(self._format_path_for_display(self._last_run_path))
            self._snapshot_last_run_var.set("\n".join(line for line in lines if line))
        else:
            self._snapshot_last_run_var.set("Estimator not yet run.")

        next_interval: Optional[int]
        if running:
            next_interval = 1000
        elif self._last_run_completed_at:
            next_interval = 60000
        else:
            next_interval = None

        if next_interval is not None:
            try:
                self._snapshot_timer_job = self.root.after(next_interval, self._refresh_workflow_snapshot)
            except tk.TclError:
                self._snapshot_timer_job = None
        else:
            self._snapshot_timer_job = None

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
            self._pipeline_started_at = datetime.now()
            self._set_drop_enabled(False)
            self._set_status(
                "Running estimator…",
                "Processing workbook data and building pricing intelligence.",
                "accent_active",
            )
        else:
            self._set_drop_enabled(True)
            self._update_run_button_state()
        self._refresh_workflow_snapshot()

    def _clear_last_results(self) -> None:
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("Estimator busy", "Please wait for the current run to finish.")
            return

        self._selected_path = None
        self._current_path = None
        self._drop_hover = False
        self._stop_run_log_animation()
        self._set_status("Ready to Start", self._initial_status, "success")
        self.etcc_var.set("")
        self.district_var.set("")
        self.district_combo.set("")
        self._last_valid_contract_filter = 50.0
        self._format_contract_filter_display(50.0)
        self._update_drop_target(None)
        self._update_run_button_state()
        self._stop_run_log_animation()
        self.log_widget.configure(state=tk.NORMAL)
        self.log_widget.delete("1.0", tk.END)
        self.log_widget.configure(state=tk.DISABLED)
        self._stop_snapshot_timer()
        self._pipeline_started_at = None
        self._last_run_completed_at = None
        self._last_run_duration = None
        self._last_run_success = None
        self._last_run_path = None
        self._log_entry_count = 0
        self._last_log_message = None
        self._refresh_workflow_snapshot()

    def _append_log(self, text: str) -> None:
        normalized = text.lower()
        tags = ["base"]
        if any(keyword in normalized for keyword in ("error", "failed", "traceback")):
            tags.append("error")
        elif any(keyword in normalized for keyword in ("complete", "success", "finished", "done")):
            tags.append("success")
        elif any(keyword in normalized for keyword in ("start", "running", "launch", "processing")):
            tags.append("accent")

        self.log_widget.configure(state=tk.NORMAL)
        self.log_widget.insert(tk.END, text + "\n", tuple(tags))
        self.log_widget.see(tk.END)
        self.log_widget.configure(state=tk.DISABLED)
        self._log_entry_count += 1
        stripped = text.strip()
        if stripped:
            meaningful_lines = [line for line in stripped.splitlines() if line.strip()]
            if meaningful_lines:
                self._last_log_message = meaningful_lines[-1]
        self._refresh_workflow_snapshot()

    def _build_run_log_animation_messages(
        self,
        workbook: Path,
        expected_cost: float,
        district_display: str,
        region_id: int,
        contract_filter_pct: float,
    ) -> List[str]:
        rounded_filter = round(contract_filter_pct, 2)
        if abs(rounded_filter - round(rounded_filter)) < 1e-6:
            filter_display = f"+/-{int(round(rounded_filter))}%"
        else:
            trimmed = f"{rounded_filter:.2f}".rstrip("0").rstrip(".")
            filter_display = f"+/-{trimmed}%"
        workbook_name = workbook.name
        etcc_display = f"${expected_cost:,.2f}"
        return [
            f"Prepping workbook '{workbook_name}' for the estimator...",
            "Indexing quantity groups and validating workbook sheets...",
            f"Verifying district alignment for {district_display} (Region {region_id})...",
            f"Pulling BidTabs comparisons within {filter_display} of target quantities...",
            f"Calibrating pricing models around {etcc_display} ETCC target...",
            "Synthesizing AI guidance and design memos...",
            "Compiling estimate package and quality checks...",
        ]

    def _start_run_log_animation(
        self,
        workbook: Path,
        expected_cost: float,
        district_display: str,
        region_id: int,
        contract_filter_pct: float,
    ) -> None:
        self._stop_run_log_animation()
        messages = self._build_run_log_animation_messages(
            workbook, expected_cost, district_display, region_id, contract_filter_pct
        )
        self._run_log_rotation_messages = messages
        if not self._run_log_rotation_messages:
            return
        self._run_log_rotation_index = 0
        self._advance_run_log_animation()

    def _advance_run_log_animation(self) -> None:
        if not self._run_log_rotation_messages:
            return
        if self._run_log_rotation_index >= len(self._run_log_rotation_messages):
            self._run_log_rotation_index = 0
        message = self._run_log_rotation_messages[self._run_log_rotation_index]
        self._run_log_rotation_index += 1
        self._show_rotating_log_message(message)
        try:
            self._run_log_animation_job = self.root.after(2000, self._advance_run_log_animation)
        except tk.TclError:
            self._run_log_animation_job = None

    def _show_rotating_log_message(self, message: str) -> None:
        if not hasattr(self, "log_widget"):
            return
        self.log_widget.configure(state=tk.NORMAL)
        try:
            ranges = self.log_widget.tag_ranges("animation")
            # Remove previous animated message (if any) before inserting the next one.
            while ranges:
                start, end = ranges[0], ranges[1]
                self.log_widget.delete(start, end)
                ranges = self.log_widget.tag_ranges("animation")
            self.log_widget.insert(tk.END, message + "\n", ("base", "animation"))
            self.log_widget.see(tk.END)
        finally:
            self.log_widget.configure(state=tk.DISABLED)
        self._last_log_message = message
        self._refresh_workflow_snapshot()

    def _stop_run_log_animation(self) -> None:
        if self._run_log_animation_job is not None:
            try:
                self.root.after_cancel(self._run_log_animation_job)
            except tk.TclError:
                pass
            self._run_log_animation_job = None
        if not hasattr(self, "log_widget"):
            return
        self.log_widget.configure(state=tk.NORMAL)
        try:
            ranges = self.log_widget.tag_ranges("animation")
            while ranges:
                start, end = ranges[0], ranges[1]
                self.log_widget.delete(start, end)
                ranges = self.log_widget.tag_ranges("animation")
        finally:
            self.log_widget.configure(state=tk.DISABLED)
        self._refresh_workflow_snapshot()

    def _handle_drop(self, event: tk.Event) -> None:  # pragma: no cover - UI event
        if not self._drop_enabled:
            return

        paths = _split_dropped_paths(getattr(event, "data", ""))
        for path in paths:
            if path.is_file() and path.name.endswith("_project_quantities.xlsx"):
                self._select_workbook(path)
                return
        messagebox.showerror("Invalid file", "Please drop a *_project_quantities.xlsx workbook.")

    def _handle_drop_click(self, _event: Optional[tk.Event] = None) -> None:  # pragma: no cover - UI event
        if not self._drop_enabled:
            return
        self._browse_file()

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
        self._drop_hover = False
        detail = "Fill in the project inputs below, then run the estimator when ready."
        self._set_status("Workbook ready", detail, "accent_active")
        self._update_drop_target(path)
        self._update_run_button_state()
        self._refresh_workflow_snapshot()

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
        if not self.alt_seek_var.get():
            self._append_log(
                "Alternate seek disabled for this run; missing pay items will retain their NO DATA baseline."
            )
        self._set_running(True)
        self._start_run_log_animation(path, expected_cost, district_display, region_id, contract_filter_pct)

        self._worker = threading.Thread(
            target=self._run_pipeline,
            args=(path, expected_cost, district_name, region_id, contract_filter_pct, bool(self.alt_seek_var.get())),
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
        alt_seek_enabled: bool,
    ) -> None:
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()

        env_overrides = dict(os.environ)
        env_overrides["QUANTITIES_XLSX"] = str(path)
        env_overrides["EXPECTED_TOTAL_CONTRACT_COST"] = f"{expected_cost:.2f}"
        env_overrides["PROJECT_DISTRICT"] = district_name
        env_overrides["PROJECT_REGION"] = str(region_id)
        env_overrides["BIDTABS_CONTRACT_FILTER_PCT"] = f"{contract_filter_pct:.6f}"
        if alt_seek_enabled:
            env_overrides.pop("DISABLE_ALT_SEEK", None)
        else:
            env_overrides["DISABLE_ALT_SEEK"] = "1"

        runtime_cfg = load_runtime_config(env_overrides, None)

        try:
            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                exit_code = run_estimator(runtime_config=runtime_cfg)

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
        self._stop_run_log_animation()
        finished_at = datetime.now()
        success = result.level == "info"
        self._last_run_completed_at = finished_at
        self._last_run_duration = (
            finished_at - self._pipeline_started_at if self._pipeline_started_at else None
        )
        self._pipeline_started_at = None
        self._last_run_success = success
        if self._current_path is not None:
            self._last_run_path = self._current_path
        if result.level == "info":
            self._append_log(result.message)
            detail = (
                f"Estimator finished for {self._current_path.name}."
                if self._current_path is not None
                else "Estimator run completed successfully."
            )
            self._set_status("Run complete", detail, "success")
            self._show_completion_dialog(result.message)
        else:
            self._append_log(result.message)
            if result.details:
                self._append_log(result.details)
            self._set_status(
                "Run failed",
                "Review the run log for diagnostic details and try again.",
                "error",
            )
            messagebox.showerror("Estimator error", result.message)
        self._refresh_workflow_snapshot()

    # ---------------------------------------------------------------- Main --
    def run(self) -> None:  # pragma: no cover - UI loop
        self.root.mainloop()


def main() -> None:  # pragma: no cover - entry point
    app = EstimatorApp()
    app.run()


if __name__ == "__main__":  # pragma: no cover - script mode
    main()
