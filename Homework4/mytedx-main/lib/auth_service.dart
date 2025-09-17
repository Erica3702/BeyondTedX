import 'package:http/http.dart' as http;
import 'dart:convert';

// Autentifica un utente tramite username e password.
// Restituisce una mappa con i dati dell'utente in caso di successo.
// Lancia un'eccezione in caso di fallimento.
Future<Map<String, dynamic>> loginUser(String username, String password) async {
  final url = Uri.parse(
    'https://9shf0o85h8.execute-api.us-east-1.amazonaws.com/auth/login',
  );

  print("--- Tentativo di login per l'utente: $username ---");

  try {
    final response = await http.post(
      url,
      headers: <String, String>{
        'Content-Type': 'application/json; charset=UTF-8',
      },
      body: jsonEncode(<String, String>{
        'username': username,
        'password': password,
      }),
    );

    if (response.statusCode == 200) {
      final responseBody = json.decode(utf8.decode(response.bodyBytes));
      print("--- Login Riuscito: ${responseBody['message']} ---");
      return responseBody;
    } else if (response.statusCode == 401 || response.statusCode == 400) {
      final errorBody = json.decode(utf8.decode(response.bodyBytes));
      throw Exception(errorBody['message'] ?? 'Credenziali non valide.');
    } else {
      throw Exception('Errore del server. Status code: ${response.statusCode}');
    }
  } catch (e) {
    print("--- Errore durante il login: $e ---");
    throw Exception(
      'Impossibile connettersi al server. Controlla la tua connessione.',
    );
  }
}
