import 'dart:convert';

List<LearningPath> learningPathFromJson(String str) => List<LearningPath>.from(
  json.decode(str).map((x) => LearningPath.fromJson(x)),
);

class LearningPath {
  final String id;
  final List<String> talkIds;
  final String pathTitle;
  final int talksCount;
  final int totalDurationMinutes;
  final String badgeToUnlock;
  final String pathDescription;
  final String mainTag;

  LearningPath({
    required this.id,
    required this.talkIds,
    required this.pathTitle,
    required this.talksCount,
    required this.totalDurationMinutes,
    required this.badgeToUnlock,
    required this.pathDescription,
    required this.mainTag,
  });

  factory LearningPath.fromJson(Map<String, dynamic> json) => LearningPath(
    id: json["_id"] ?? '', // Se _id è null, usa una stringa vuota

    talkIds: json["talk_ids"] == null
        ? [] // Se talk_ids è null, usa una lista vuota
        : List<String>.from(json["talk_ids"].map((x) => x)),

    pathTitle: json["path_title"] ?? 'Percorso senza titolo',

    talksCount: json["talks_count"] ?? 0, // Se talks_count è null, usa 0

    totalDurationMinutes: json["total_duration_minutes"] ?? 0,

    badgeToUnlock: json["badge_to_unlock"] ?? '',

    pathDescription:
        json["path_description"] ?? 'Nessuna descrizione disponibile.',

    mainTag: json["main_tag"] ?? '',
  );
}
