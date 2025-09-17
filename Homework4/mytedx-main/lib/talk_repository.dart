import 'package:http/http.dart' as http;
import 'dart:convert';
import 'models/talk.dart';
import 'learning_path.dart';

Future<List<Talk>> initEmptyList() async {
  Iterable list = json.decode("[]");
  var talks = list.map((model) => Talk.fromJSON(model)).toList();
  return talks;
}

Future<List<Talk>> getTalksByTag(String tag, int page) async {
  var url = Uri.parse(
    'https://86jnvzgic3.execute-api.us-east-1.amazonaws.com/v8/talks',
  );

  final http.Response response = await http.post(
    url,
    headers: <String, String>{'Content-Type': 'application/json'},
    body: jsonEncode(<String, Object>{
      'tag': tag,
      'page': page,
      'doc_per_page': 6,
    }),
  );

  if (response.statusCode == 200) {
    final body = utf8.decode(response.bodyBytes);
    print("--- RISPOSTA GREZZA DAL SERVER (getTalksByTag): $body ---");

    final Map<String, dynamic> responseData = json.decode(body);

    final List<dynamic> jsonList = responseData['data'] as List<dynamic>;

    return jsonList.map((json) => Talk.fromJSON(json)).toList();
  } else {
    throw Exception('Failed to load talks');
  }
}

Future<List<LearningPath>> fetchLearningPaths() async {
  final response = await http.get(
    Uri.parse(
      'https://3iqxzawj58.execute-api.us-east-1.amazonaws.com/p1/learning-paths',
    ),
  );

  if (response.statusCode == 200) {
    return learningPathFromJson(response.body);
  } else {
    throw Exception('Failed to load learning paths');
  }
}
