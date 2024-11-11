-- Uploads named media.bin were stored as Telegram file ids. Replaying those
-- ids keeps the old name, and phones will not render it. The next send uploads
-- the bytes again under photo.jpg / video.mp4 / audio.mp3.
UPDATE telegram_deliveries
SET invalidated_at = now()
WHERE invalidated_at IS NULL;
