import 'package:http/http.dart' as http;
import 'dart:convert';
import 'models/talk.dart';

Future<List<Talk>> getWatchNextById(String talkId, int page) async {
  // Costruiamo l'URL inserendo dinamicamente l'ID del talk.
  final url = Uri.parse(
    'https://86jnvzgic3.execute-api.us-east-1.amazonaws.com/v8/talks/$talkId/watch-next',
  );

  print("--- URL Chiamato: $url ---");

  try {
    final http.Response response = await http.get(url);

    if (response.statusCode == 200) {
      final body = utf8.decode(response.bodyBytes);
      final List<dynamic> jsonList = json.decode(body);
      return jsonList.map((json) => Talk.fromJSON(json)).toList();
    } else {
      throw Exception(
        'Failed to load watch next talks. Status code: ${response.statusCode}',
      );
    }
  } catch (e) {
    // Gestisce errori di rete o di parsing
    throw Exception('An error occurred: $e');
  }
}
