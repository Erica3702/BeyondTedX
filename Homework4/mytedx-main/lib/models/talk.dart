// Nel tuo file models/talk.dart

class Talk {
  final String id;
  final String title;
  final String details;
  final String mainSpeaker;
  final String url;
  final List<String> keyPhrases;

  // Aggiungi un costruttore standard per il factory
  Talk({
    required this.id,
    required this.title,
    required this.details,
    required this.mainSpeaker,
    required this.url,
    required this.keyPhrases,
  });

  factory Talk.fromJSON(Map<String, dynamic> jsonMap) {
    String speakerName;
    final speakersData = jsonMap['speakers'];

    if (speakersData is List) {
      // Se è una Lista, unisce i nomi in una stringa (es. "Nome1, Nome2")
      speakerName = speakersData.join(', ');
    } else if (speakersData is String) {
      // Se è già una Stringa, la usa
      speakerName = speakersData;
    } else {
      // Altrimenti, usa il valore di default
      speakerName = "Unknown Speaker";
    }

    return Talk(
      id: (jsonMap['id'] ?? jsonMap['_id'] ?? '').toString(),
      title: jsonMap['title'] ?? 'No Title',
      details: jsonMap['description'] ?? 'No Description',
      mainSpeaker: speakerName.isEmpty ? "Unknown Speaker" : speakerName,
      url: jsonMap['url'] ?? "",
      keyPhrases:
          (jsonMap['comprehend_analysis']?['KeyPhrases'] as List<dynamic>?)
              ?.map((e) => e.toString())
              .toList() ??
          [],
    );
  }
}
