import 'package:flutter/material.dart';
import 'talk_repository.dart';
import 'models/talk.dart';
import 'watch_next_screen.dart';
import 'login.dart';
import 'learning_path.dart';

void main() => runApp(const MyApp());

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'BeyondTEDx',
      theme: ThemeData(
        primarySwatch: Colors.red,
        colorScheme: ColorScheme.fromSwatch(
          primarySwatch: Colors.red,
        ).copyWith(secondary: Colors.redAccent),
      ),
      debugShowCheckedModeBanner: false,
      home: const LoginScreen(), //schermata iniziale
    );
  }
}

class MyHomePage extends StatefulWidget {
  const MyHomePage({super.key});

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  final TextEditingController _controller = TextEditingController();
  Future<List<Talk>>? _talks;
  int page = 1;
  bool init = true;

  late Future<List<LearningPath>> _learningPathsFuture;

  @override
  void initState() {
    super.initState();
    _talks = Future.value([]);

    _learningPathsFuture = fetchLearningPaths();

    _controller.addListener(() {
      setState(() {});
    });
  }

  void _getTalksByTag({bool loadMore = false}) {
    if (_controller.text.trim().isEmpty) return;
    if (!loadMore) {
      page = 1;
    }
    setState(() {
      init = false;
      _talks = getTalksByTag(_controller.text.trim(), page);
    });
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
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
      ),
      body: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Padding(
            padding: const EdgeInsets.fromLTRB(16, 16, 16, 12),
            child: Container(
              decoration: BoxDecoration(
                color: Colors.grey[100],
                borderRadius: BorderRadius.circular(10),
              ),
              child: TextField(
                controller: _controller,
                decoration: InputDecoration(
                  hintText: 'Cerca per tag (es. Sun)',
                  prefixIcon: const Icon(Icons.search, color: Colors.red),
                  suffixIcon: _controller.text.isNotEmpty
                      ? IconButton(
                          icon: const Icon(Icons.clear, color: Colors.grey),
                          onPressed: () {
                            _controller.clear();
                            setState(() {
                              init = true;
                              _talks = Future.value([]);
                            });
                          },
                        )
                      : null,
                  border: InputBorder.none,
                  contentPadding: const EdgeInsets.symmetric(
                    vertical: 15.0,
                    horizontal: 20.0,
                  ),
                ),
                onSubmitted: (value) => _getTalksByTag(),
              ),
            ),
          ),
          // L'area dei risultati della ricerca
          Expanded(
            flex: 1, // Occupa metà dello spazio rimanente
            child: FutureBuilder<List<Talk>>(
              future: _talks,
              builder: (context, snapshot) {
                if (init) {
                  return const Center(
                    child: Text(
                      'Inizia una ricerca per tag',
                      style: TextStyle(color: Colors.grey),
                    ),
                  );
                }
                if (snapshot.connectionState == ConnectionState.waiting) {
                  return const Center(
                    child: CircularProgressIndicator(color: Colors.red),
                  );
                }
                if (snapshot.hasError) {
                  return Center(
                    child: Text(
                      "Errore: ${snapshot.error}",
                      style: const TextStyle(color: Colors.red),
                    ),
                  );
                }
                if (!snapshot.hasData || snapshot.data!.isEmpty) {
                  return const Center(
                    child: Text(
                      "Nessun risultato trovato.",
                      style: TextStyle(color: Colors.grey, fontSize: 16),
                    ),
                  );
                }
                final talks = snapshot.data!;
                return ListView.builder(
                  padding: const EdgeInsets.symmetric(horizontal: 8),
                  itemCount: talks.length,
                  itemBuilder: (context, index) {
                    final talk = talks[index];
                    return Card(
                      child: ListTile(
                        title: Text(
                          talk.title,
                          style: const TextStyle(fontWeight: FontWeight.bold),
                        ),
                        subtitle: Text(talk.mainSpeaker),
                        onTap: () => Navigator.push(
                          context,
                          MaterialPageRoute(
                            builder: (context) =>
                                WatchNextScreen(talkId: talk.id),
                          ),
                        ),
                      ),
                    );
                  },
                );
              },
            ),
          ),

          const Padding(
            padding: EdgeInsets.symmetric(horizontal: 16.0, vertical: 8.0),
            child: Text(
              'Oppure scopri i percorsi formativi',
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
          ),

          // METÀ INFERIORE: PERCORSI FORMATIVI
          Expanded(
            flex: 1, // Occupa l'altra metà dello spazio rimanente
            child: FutureBuilder<List<LearningPath>>(
              future: _learningPathsFuture,
              builder: (context, snapshot) {
                if (snapshot.connectionState == ConnectionState.waiting) {
                  return const Center(child: CircularProgressIndicator());
                }
                if (snapshot.hasError) {
                  return Center(child: Text('Errore: ${snapshot.error}'));
                }
                if (!snapshot.hasData || snapshot.data!.isEmpty) {
                  return const Center(child: Text('Nessun percorso trovato.'));
                }

                final paths = snapshot.data!;
                return ListView.builder(
                  padding: const EdgeInsets.symmetric(horizontal: 8),
                  itemCount: paths.length,
                  itemBuilder: (context, index) {
                    final path = paths[index];
                    return Card(
                      elevation: 2,
                      margin: const EdgeInsets.symmetric(
                        vertical: 6,
                        horizontal: 8,
                      ),
                      child: ListTile(
                        title: Text(
                          path.pathTitle,
                          style: const TextStyle(fontWeight: FontWeight.bold),
                        ),
                        subtitle: Text(
                          '${path.talksCount} talk • ${path.totalDurationMinutes} min',
                        ),
                        trailing: const Icon(
                          Icons.arrow_forward_ios,
                          size: 16,
                          color: Colors.red,
                        ),
                        onTap: () {
                          // Azione quando un utente clicca un percorso
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
