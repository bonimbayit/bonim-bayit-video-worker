from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, HttpUrl
from typing import Optional, List
import uuid
import shutil
import subprocess
from pathlib import Path
from PIL import Image, ImageStat, ImageFilter

app = FastAPI(title="Bonim Bayit Video Frame Worker")

BASE_DIR = Path(__file__).resolve().parent
TMP_DIR = BASE_DIR / "tmp"
FRAMES_DIR = BASE_DIR / "frames"

TMP_DIR.mkdir(exist_ok=True)
FRAMES_DIR.mkdir(exist_ok=True)

app.mount("/frames", StaticFiles(directory=str(FRAMES_DIR)), name="frames")


class ExtractRequest(BaseModel):
    video_url: HttpUrl
    post_id: Optional[int] = None
    article_type: Optional[str] = None
    hook: Optional[str] = None
    max_seconds: int = 120
    interval_seconds: int = 4
    max_candidates: int = 8


def run_cmd(cmd: List[str]) -> str:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(e.stderr.strip() or e.stdout.strip() or "Command failed")


def format_timecode(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    return f"{m:02d}:{s:02d}"


def download_video(video_url: str, work_dir: Path) -> Path:
    output_template = str(work_dir / "video.%(ext)s")

    cmd = [
        "yt-dlp",
        "--extractor-args", "youtube:player_client=android",
        "-f", "mp4/best",
        "-o", output_template,
        video_url
    ]

    run_cmd(cmd)

    for f in work_dir.iterdir():
        if f.name.startswith("video."):
            return f

    raise RuntimeError("Video file was not downloaded")


def extract_frames(video_path: Path, work_dir: Path, max_seconds: int, interval_seconds: int):
    frame_pattern = str(work_dir / "frame_%03d.jpg")

    cmd = [
        "ffmpeg",
        "-y",
        "-i", str(video_path),
        "-t", str(max_seconds),
        "-vf", f"fps=1/{interval_seconds}",
        frame_pattern
    ]

    run_cmd(cmd)

    return sorted(work_dir.glob("frame_*.jpg"))


def image_sharpness_score(img: Image.Image) -> float:
    gray = img.convert("L")
    edges = gray.filter(ImageFilter.FIND_EDGES)
    stat = ImageStat.Stat(edges)
    return stat.var[0] if stat.var else 0


def image_brightness_score(img: Image.Image) -> float:
    gray = img.convert("L")
    stat = ImageStat.Stat(gray)
    return stat.mean[0] if stat.mean else 0


def image_contrast_score(img: Image.Image) -> float:
    gray = img.convert("L")
    stat = ImageStat.Stat(gray)
    return stat.stddev[0] if stat.stddev else 0


def score_frame(frame_path: Path, article_type: Optional[str] = None) -> int:
    img = Image.open(frame_path)

    sharpness = image_sharpness_score(img)
    brightness = image_brightness_score(img)
    contrast = image_contrast_score(img)

    score = 0

    score += min(40, int(sharpness / 3))

    if 70 <= brightness <= 190:
        score += 25
    elif 50 <= brightness <= 220:
        score += 15
    else:
        score -= 10

    score += min(20, int(contrast))

    if article_type == "material_comparison":
        score += 5
    elif article_type == "technical_system":
        score += 5
    elif article_type == "on_site_construction":
        score += 5

    return max(0, min(100, score))


def convert_best_frames(frame_paths, article_type, max_candidates, interval_seconds):
    scored = []
    for i, fp in enumerate(frame_paths):
        score = score_frame(fp, article_type)
        time_sec = i * interval_seconds
        scored.append((fp, score, time_sec))

    scored.sort(key=lambda x: x[1], reverse=True)
    best = scored[:max_candidates]

    result = []
    for fp, score, time_sec in best:
        out_name = f"{uuid.uuid4().hex}.webp"
        out_path = FRAMES_DIR / out_name

        img = Image.open(fp).convert("RGB")
        img.save(out_path, "WEBP", quality=88)

        result.append({
            "id": out_name.split(".")[0],
            "image_url": f"/frames/{out_name}",
            "timecode": format_timecode(time_sec),
            "score": score
        })

    return result


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/extract")
def extract(req: ExtractRequest):
    job_id = uuid.uuid4().hex
    work_dir = TMP_DIR / job_id
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        video_path = download_video(str(req.video_url), work_dir)
        frames = extract_frames(video_path, work_dir, req.max_seconds, req.interval_seconds)

        if not frames:
            raise HTTPException(status_code=400, detail="No frames extracted from video")

        best_frames = convert_best_frames(
            frames,
            req.article_type,
            req.max_candidates,
            req.interval_seconds
        )

        if not best_frames:
            raise HTTPException(status_code=400, detail="No strong frame candidates found")

        return {
            "success": True,
            "frames": best_frames
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)