 import base64             
  import hashlib
  import os                                                                                                                                                                                                                                
  import re
  import shutil                                                                                                                                                                                                                            
  import subprocess                                         
  import tempfile
  import threading
  import time                                                                                                                                                                                                                              
  import uuid
  from pathlib import Path                                                                                                                                                                                                                 
  from typing import List, Optional
                                                                                                                                                                                                                                           
  from fastapi import FastAPI                                                                                                                                                                                                              
  from fastapi.responses import JSONResponse
  from fastapi.staticfiles import StaticFiles                                                                                                                                                                                              
  from pydantic import BaseModel, Field
  from starlette.exceptions import HTTPException as StarletteHTTPException                                                                                                                                                                 


  FRAMES_DIR = Path(os.environ.get("FRAMES_DIR", "/tmp/frames"))
  FRAMES_DIR.mkdir(parents=True, exist_ok=True)

  COOKIES_PATH = "/tmp/youtube_cookies.txt"
  _cookies_lock = threading.Lock()
  _cookies_ready = False

  FRAME_TTL_SECONDS = int(os.environ.get("FRAME_TTL_SECONDS", "3600"))                                                                                                                                                                     

  RESTRICTED_SIGNALS = (                                                                                                                                                                                                                   
      "sign in to confirm",
      "confirm you",                                                                                                                                                                                                                       
      "not a bot",
      "age-restricted",                                                                                                                                                                                                                    
      "age restricted",
      "members-only",                                                                                                                                                                                                                      
      "members only",                                       
      "private video",
      "video unavailable",                                                                                                                                                                                                                 
      "requires authentication",
      "login required",                                                                                                                                                                                                                    
      "this video is available to this channel's members",
  )                                                                                                                                                                                                                                        


  app = FastAPI(title="bonim-bayit-video-worker")
  app.mount("/frames", StaticFiles(directory=str(FRAMES_DIR)), name="frames")


  # ---------- models ----------

  class ExtractRequest(BaseModel):
      video_url: str
      post_id: Optional[int] = 0
      article_type: Optional[str] = ""
      hook: Optional[str] = ""
      max_seconds: int = Field(default=120, ge=10, le=600)
      interval_seconds: int = Field(default=4, ge=1, le=30)                                                                                                                                                                                
      max_candidates: int = Field(default=8, ge=1, le=24)


  # ---------- helpers ----------

  def ensure_cookies_file() -> Optional[str]:
      """Materialize YTDLP_COOKIES_B64 into a Netscape-format cookies file.
      Temporary fallback; can be replaced with a secret-mount later."""
      global _cookies_ready
      b64 = os.environ.get("YTDLP_COOKIES_B64", "").strip()                                                                                                                                                                                
      if not b64:
          return None
      with _cookies_lock:
          if _cookies_ready and os.path.exists(COOKIES_PATH):                                                                                                                                                                              
              return COOKIES_PATH
          try:
              data = base64.b64decode(b64, validate=True)                                                                                                                                                                                  
          except Exception:
              return None
          fd, tmp = tempfile.mkstemp(prefix="ytc_", suffix=".txt", dir="/tmp")
          with os.fdopen(fd, "wb") as f:
              f.write(data)                                                                                                                                                                                                                
          os.chmod(tmp, 0o600)
          os.replace(tmp, COOKIES_PATH)                                                                                                                                                                                                    
          _cookies_ready = True
          return COOKIES_PATH


  def _classify_ytdlp_error(stderr: str, stdout: str) -> Optional[dict]:
      blob = ((stderr or "") + "\n" + (stdout or "")).lower()                                                                                                                                                                              
      if any(sig in blob for sig in RESTRICTED_SIGNALS):
          return {                                                                                                                                                                                                                         
              "error": "youtube_access_restricted",
              "message": "YouTube blocked extraction for this video (sign-in or restricted playback required).",
          }                                                                                                                                                                                                                                
      return None


  def resolve_stream_url(video_url: str) -> str:
      format_selector = (                                                                                                                                                                                                                  
          "best[ext=mp4][protocol=https][height<=720]/"
          "best[ext=mp4][protocol=https]/"
          "best[height<=720][ext=mp4]/"
          "best[height<=720]/best"
      )
      cmd = [
          "yt-dlp",                                                                                                                                                                                                                        
          "--no-warnings",
          "--no-playlist",                                                                                                                                                                                                                 
          "--extractor-args", "youtube:player_client=android",
          "-f", format_selector,                                                                                                                                                                                                           
          "--get-url",
          video_url,                                                                                                                                                                                                                       
      ]
      cookies = ensure_cookies_file()                                                                                                                                                                                                      
      if cookies:                                           
          cmd[1:1] = ["--cookies", cookies]

      try:
          proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
      except subprocess.TimeoutExpired:                                                                                                                                                                                                    
          raise StarletteHTTPException(status_code=504, detail={
              "error": "youtube_timeout",                                                                                                                                                                                                  
              "message": "yt-dlp timed out resolving the stream URL.",
          })                                                                                                                                                                                                                               

      if proc.returncode != 0 or not (proc.stdout or "").strip():                                                                                                                                                                          
          restricted = _classify_ytdlp_error(proc.stderr or "", proc.stdout or "")
          if restricted:                                                                                                                                                                                                                   
              raise StarletteHTTPException(status_code=502, detail=restricted)
          raise StarletteHTTPException(status_code=502, detail={                                                                                                                                                                           
              "error": "ytdlp_failed",
              "message": (proc.stderr or proc.stdout or "unknown yt-dlp error").strip()[:400],
          })                                                                                                                                                                                                                               

      for line in proc.stdout.splitlines():                                                                                                                                                                                                
          line = line.strip()
          if line.startswith("http"):                                                                                                                                                                                                      
              return line                                   
                                                                                                                                                                                                                                           
      raise StarletteHTTPException(status_code=502, detail={                                                                                                                                                                               
          "error": "ytdlp_no_url",
          "message": "yt-dlp produced no stream URL.",
      })                                                                                                                                                                                                                                   


  def sample_frames(stream_url: str, max_seconds: int, interval_seconds: int, max_candidates: int, job_id: str) -> List[dict]:
      job_dir = FRAMES_DIR / job_id                                                                                                                                                                                                        
      job_dir.mkdir(parents=True, exist_ok=True)
      pattern = str(job_dir / "frame-%03d.jpg")

      cmd = [
          "ffmpeg", "-y",                                                                                                                                                                                                                  
          "-ss", "0",
          "-t", str(max_seconds),                                                                                                                                                                                                          
          "-i", stream_url,
          "-vf", f"fps=1/{interval_seconds},scale=1280:-2",
          "-frames:v", str(max_candidates * 5),
          pattern,                                                                                                                                                                                                                         
      ]
      try:                                                                                                                                                                                                                                 
          proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
      except subprocess.TimeoutExpired:                                                                                                                                                                                                    
          raise StarletteHTTPException(status_code=504, detail={
              "error": "ffmpeg_timeout",
              "message": "ffmpeg timed out while sampling frames.",                                                                                                                                                                        
          })
      if proc.returncode != 0:                                                                                                                                                                                                             
          raise StarletteHTTPException(status_code=502, detail={
              "error": "ffmpeg_failed",                                                                                                                                                                                                    
              "message": (proc.stderr or proc.stdout or "unknown ffmpeg error").strip()[:400],
          })                                                                                                                                                                                                                               

      jpgs = sorted(job_dir.glob("frame-*.jpg"))                                                                                                                                                                                           
      if not jpgs:                                          
          raise StarletteHTTPException(status_code=502, detail={
              "error": "no_frames",                                                                                                                                                                                                        
              "message": "ffmpeg produced no frames.",
          })                                                                                                                                                                                                                               

      scored: List[dict] = []                                                                                                                                                                                                              
      seen_hashes: List[str] = []
      for i, jpg in enumerate(jpgs):
          sec = i * interval_seconds                                                                                                                                                                                                       
          score = score_frame(jpg)
          h = phash(jpg)                                                                                                                                                                                                                   
          if any(hamming(h, prev) < 6 for prev in seen_hashes):
              continue
          seen_hashes.append(h)

          webp_path = job_dir / f"{jpg.stem}.webp"                                                                                                                                                                                         
          if not convert_to_webp(jpg, webp_path):
              continue
          jpg.unlink(missing_ok=True)

          scored.append({                                                                                                                                                                                                                  
              "id": f"frame-{i+1}",
              "image_url": f"/frames/{job_id}/{webp_path.name}",
              "timecode": f"{sec // 60:02d}:{sec % 60:02d}",
              "score": score,                                                                                                                                                                                                              
          })

      scored.sort(key=lambda x: x["score"], reverse=True)   
      return scored[:max_candidates]
                                                                                                                                                                                                                                           

  def convert_to_webp(src: Path, dst: Path) -> bool:
      cmd = ["ffmpeg", "-y", "-i", str(src), "-q:v", "82", str(dst)]
      try:                                                                                                                                                                                                                                 
          proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
      except subprocess.TimeoutExpired:
          return False
      return proc.returncode == 0 and dst.exists()


  def score_frame(path: Path) -> int:
      try:                                                                                                                                                                                                                                 
          from PIL import Image, ImageFilter, ImageStat     
      except Exception:
          return 50
      try:                                                                                                                                                                                                                                 
          with Image.open(path) as im:
              im = im.convert("L")                                                                                                                                                                                                         
              edges = im.filter(ImageFilter.FIND_EDGES)
              sharp = ImageStat.Stat(edges).stddev[0]
              brightness = ImageStat.Stat(im).mean[0]                                                                                                                                                                                      
              s = int(min(100, max(0, sharp * 2)))
              if brightness < 25 or brightness > 235:
                  s -= 20
              return max(0, min(100, s))
      except Exception:                                                                                                                                                                                                                    
          return 50
                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                           
  def phash(path: Path) -> str:
      try:
          from PIL import Image                                                                                                                                                                                                            
          with Image.open(path) as im:
              im = im.convert("L").resize((8, 8))                                                                                                                                                                                          
              pixels = list(im.getdata())
              avg = sum(pixels) / len(pixels)                                                                                                                                                                                              
              bits = "".join("1" if p >= avg else "0" for p in pixels)
              return f"{int(bits, 2):016x}"
      except Exception:                                                                                                                                                                                                                    
          return hashlib.md5(path.read_bytes()).hexdigest()[:16]


  def hamming(a: str, b: str) -> int:
      try:                                                                                                                                                                                                                                 
          return bin(int(a, 16) ^ int(b, 16)).count("1")
      except Exception:                                                                                                                                                                                                                    
          return 64


  def cleanup_old_jobs():
      cutoff = time.time() - FRAME_TTL_SECONDS                                                                                                                                                                                             
      for child in FRAMES_DIR.iterdir():                    
          try:
              if child.is_dir() and child.stat().st_mtime < cutoff:                                                                                                                                                                        
                  shutil.rmtree(child, ignore_errors=True)
          except Exception:                                                                                                                                                                                                                
              pass


  # ---------- routes ----------
                                                                                                                                                                                                                                           
  @app.get("/health")
  def health():
      return {"ok": True, "cookies": bool(os.environ.get("YTDLP_COOKIES_B64", "").strip())}


  @app.post("/extract")                                                                                                                                                                                                                    
  def extract(req: ExtractRequest):                         
      cleanup_old_jobs()                                                                                                                                                                                                                   

      url = req.video_url.strip()
      if not re.match(r"^https?://", url, re.I):                                                                                                                                                                                           
          raise StarletteHTTPException(status_code=400, detail={
              "error": "bad_request",                                                                                                                                                                                                      
              "message": "video_url must be an http(s) URL.",
          })                                                                                                                                                                                                                               

      stream_url = resolve_stream_url(url)                                                                                                                                                                                                 
      job_id = uuid.uuid4().hex[:12]
      frames = sample_frames(                                                                                                                                                                                                              
          stream_url=stream_url,
          max_seconds=req.max_seconds,                                                                                                                                                                                                     
          interval_seconds=req.interval_seconds,
          max_candidates=req.max_candidates,                                                                                                                                                                                               
          job_id=job_id,
      )                                                                                                                                                                                                                                    
      return {"success": True, "frames": frames}


  @app.exception_handler(StarletteHTTPException)                                                                                                                                                                                           
  async def http_exc_handler(request, exc: StarletteHTTPException):
      if isinstance(exc.detail, dict):                                                                                                                                                                                                     
          return JSONResponse(status_code=exc.status_code, content=exc.detail)
      return JSONResponse(status_code=exc.status_code, content={                                                                                                                                                                           
          "error": "http_error",
          "message": str(exc.detail),
      })                                                                                                                                                                                                                                   


  @app.exception_handler(Exception)                         
  async def unhandled_exc_handler(request, exc: Exception):
      return JSONResponse(status_code=500, content={
          "error": "internal_error",                                                                                                                                                                                                       
          "message": str(exc)[:400],
      })                                                                                                                                                                                                                                   
