CREATE DATABASE IF NOT EXISTS checkpoint_db;
USE checkpoint_db;

CREATE TABLE IF NOT EXISTS snapshot_checkpoints (
    `range_start` bigint,
    `range_end` bigint,
    `checkpoint` bigint
);

CREATE TABLE IF NOT EXISTS `files` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `file_id` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'MD5 of file',
  `client_name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Which client upload file',
  `client_zone` varchar(8) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Which zone this file belongs to: public, private',
  `cluster` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `duration` int DEFAULT NULL COMMENT 'Duration of the audio, video file',
  `ext` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'File extension: jpg, pdf, ...',
  `fid` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Map to the file id in SeaweedFS',
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Original name of the file',
  `mime` varchar(127) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'MIME type of the file: image/jpeg, application/pdf',
  `size` int DEFAULT NULL COMMENT 'Size of the file in bytes',
  `type` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'File type',
  `height` int DEFAULT NULL COMMENT 'Height of the image/video file',
  `width` int DEFAULT NULL COMMENT 'Width of the image/video file',
  `modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last modified time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_file_id` (`file_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='store file metadata';
