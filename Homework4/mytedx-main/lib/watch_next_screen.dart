import 'package:flutter/material.dart';
import 'models/talk.dart';
import 'watch_next.dart';

class WatchNextScreen extends StatefulWidget {
  final String talkId;

  const WatchNextScreen({super.key, required this.talkId});

  @override
  State<WatchNextScreen> createState() => _WatchNextScreenState();
}

class _WatchNextScreenState extends State<WatchNextScreen> {
  late Future<List<Talk>> _watchNextFuture;

  @override
  void initState() {
    super.initState();
    _watchNextFuture = getWatchNextById(widget.talkId, 1);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Colors.white,
      appBar: AppBar(
        title: Image.asset('images/scritta.png', height: 300),
        centerTitle: true,
        backgroundColor: Colors.red,
        elevation: 0,
        iconTheme: const IconThemeData(color: Colors.white),
      ),
      body: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Padding(
            padding: const EdgeInsets.fromLTRB(16, 16, 16, 8),
            child: Text(
              'Watch next',
              style: TextStyle(
                fontSize: 22,
                fontWeight: FontWeight.bold,
                color: Colors.black87,
              ),
            ),
          ),
          Expanded(
            child: FutureBuilder<List<Talk>>(
              future: _watchNextFuture,
              builder: (context, snapshot) {
                if (snapshot.connectionState == ConnectionState.waiting) {
                  return const Center(
                    child: CircularProgressIndicator(color: Colors.red),
                  );
                }

                if (snapshot.hasError) {
                  return Center(
                    child: Text(
                      'Errore: ${snapshot.error}',
                      textAlign: TextAlign.center,
                      style: const TextStyle(color: Colors.red),
                    ),
                  );
                }

                if (!snapshot.hasData || snapshot.data!.isEmpty) {
                  return const Center(
                    child: Text(
                      'Nessun video correlato trovato.',
                      style: TextStyle(color: Colors.grey, fontSize: 16),
                    ),
                  );
                }

                final talks = snapshot.data!;
                return ListView.builder(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 8,
                    vertical: 2,
                  ),
                  itemCount: talks.length,
                  itemBuilder: (context, index) {
                    final talk = talks[index];
                    return Card(
                      elevation: 2,
                      margin: const EdgeInsets.symmetric(
                        vertical: 6,
                        horizontal: 8,
                      ),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(10),
                      ),
                      child: ListTile(
                        contentPadding: const EdgeInsets.symmetric(
                          vertical: 8,
                          horizontal: 16,
                        ),
                        leading: CircleAvatar(
                          backgroundColor: Colors.red.withOpacity(
                            0.1,
                          ), // Sfondo leggermente trasparente
                          child: const Icon(
                            Icons.mic, // Icona del microfono
                            color: Colors.red, // Colore rosso
                          ),
                        ),
                        title: Text(
                          talk.title,
                          style: const TextStyle(fontWeight: FontWeight.bold),
                        ),
                        subtitle: Text(talk.mainSpeaker),
                        trailing: const Icon(
                          Icons.arrow_forward_ios,
                          size: 16,
                          color: Colors.red,
                        ),
                        onTap: () {
                          Navigator.pushReplacement(
                            context,
                            MaterialPageRoute(
                              builder: (context) =>
                                  WatchNextScreen(talkId: talk.id),
                            ),
                          );
                        },
                      ),
                    );
                  },
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}
