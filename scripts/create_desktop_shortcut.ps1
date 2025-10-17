[CmdletBinding()]
param(
    [string]$ShortcutName = 'Cost Estimate Generator',
    [string]$VenvRelativePath = '.venv',
    [string]$LauncherRelativePath = 'launch_gui.pyw'
)

$root = (Resolve-Path -Path (Join-Path $PSScriptRoot '..')).Path
$launcherPath = Join-Path $root $LauncherRelativePath

if (-not (Test-Path -Path $launcherPath)) {
    throw "Launcher '$launcherPath' not found. Update `LauncherRelativePath` if the entry point moved."
}

$pythonwPath = $null
if ($VenvRelativePath) {
    $venvPythonw = Join-Path $root (Join-Path $VenvRelativePath 'Scripts\pythonw.exe')
    if (Test-Path -Path $venvPythonw) {
        $pythonwPath = $venvPythonw
    }
}

if (-not $pythonwPath) {
    try {
        $pythonwPath = (Get-Command pythonw.exe -ErrorAction Stop).Source
    } catch {
        throw "Unable to locate pythonw.exe. Activate the project's virtual environment or install Python."
    }
}

$iconSource = Join-Path $root 'desktop_icon\desktop-icon.jpg'
$iconTarget = Join-Path $root 'desktop_icon\desktop-icon.ico'

function Invoke-IconConversion {
    param(
        [string]$Source,
        [string]$Destination
    )

    if (-not ([System.Management.Automation.PSTypeName]'CostEstimator.IconBuilder').Type) {
        $typeDefinition = @"
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;

namespace CostEstimator
{
    public static class IconBuilder
    {
        public static void SaveIcon(string sourcePath, string destinationPath, int[] sizes)
        {
            using (var original = Image.FromFile(sourcePath))
            {
                var bitmaps = new List<Bitmap>();
                try
                {
                    foreach (var size in sizes.Distinct().OrderByDescending(x => x))
                    {
                        var canvas = new Bitmap(size, size, PixelFormat.Format32bppArgb);
                        using (var graphics = Graphics.FromImage(canvas))
                        {
                            graphics.InterpolationMode = System.Drawing.Drawing2D.InterpolationMode.HighQualityBicubic;
                            graphics.CompositingQuality = System.Drawing.Drawing2D.CompositingQuality.HighQuality;
                            graphics.SmoothingMode = System.Drawing.Drawing2D.SmoothingMode.AntiAlias;
                            graphics.Clear(Color.Transparent);

                            var scale = Math.Min(size / (float)original.Width, size / (float)original.Height);
                            var targetWidth = Math.Max(1, (int)Math.Round(original.Width * scale));
                            var targetHeight = Math.Max(1, (int)Math.Round(original.Height * scale));
                            var offsetX = (size - targetWidth) / 2f;
                            var offsetY = (size - targetHeight) / 2f;

                            graphics.DrawImage(original, offsetX, offsetY, targetWidth, targetHeight);
                        }
                        bitmaps.Add(canvas);
                    }

                    SaveAsIcon(bitmaps, destinationPath);
                }
                finally
                {
                    foreach (var bitmap in bitmaps)
                    {
                        bitmap.Dispose();
                    }
                }
            }
        }

        private static void SaveAsIcon(IList<Bitmap> images, string destinationPath)
        {
            using (var stream = new FileStream(destinationPath, FileMode.Create))
            using (var writer = new BinaryWriter(stream))
            {
                writer.Write((short)0);
                writer.Write((short)1);
                writer.Write((short)images.Count);

                int offset = 6 + (16 * images.Count);
                var imageData = new List<byte[]>(images.Count);

                foreach (var image in images)
                {
                    using (var ms = new MemoryStream())
                    {
                        image.Save(ms, ImageFormat.Png);
                        var pngBytes = ms.ToArray();
                        imageData.Add(pngBytes);

                        writer.Write((byte)(image.Width >= 256 ? 0 : image.Width));
                        writer.Write((byte)(image.Height >= 256 ? 0 : image.Height));
                        writer.Write((byte)0);
                        writer.Write((byte)0);
                        writer.Write((short)1);
                        writer.Write((short)32);
                        writer.Write(pngBytes.Length);
                        writer.Write(offset);
                        offset += pngBytes.Length;
                    }
                }

                foreach (var data in imageData)
                {
                    writer.Write(data);
                }
            }
        }
    }
}
"@
        Add-Type -TypeDefinition $typeDefinition -ReferencedAssemblies System.Drawing
    }

    [CostEstimator.IconBuilder]::SaveIcon($Source, $Destination, @(256, 128, 64, 48, 32, 16))
}

if (Test-Path -Path $iconSource) {
    $requiresConversion = $true
    if (Test-Path -Path $iconTarget) {
        $requiresConversion = (Get-Item $iconSource).LastWriteTime -gt (Get-Item $iconTarget).LastWriteTime
    }
    if ($requiresConversion) {
        Invoke-IconConversion -Source $iconSource -Destination $iconTarget
    }
} else {
    Write-Warning "Icon image '$iconSource' not found. Shortcut will use the default icon."
    $iconTarget = $null
}

$desktopPath = [Environment]::GetFolderPath('Desktop')
$shortcutPath = Join-Path $desktopPath ("$ShortcutName.lnk")

$wshShell = New-Object -ComObject WScript.Shell
$shortcut = $wshShell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $pythonwPath
$shortcut.Arguments = '"{0}"' -f $launcherPath
$shortcut.WorkingDirectory = $root
$shortcut.Description = 'Launch Cost Estimate Generator GUI'
if ($iconTarget -and (Test-Path -Path $iconTarget)) {
    $shortcut.IconLocation = $iconTarget
}
$shortcut.Save()
