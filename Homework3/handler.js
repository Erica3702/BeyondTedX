'use strict';

const connect_to_db = require('./db');
const analyzeKeyPhrases = require('./localNLP');
const Talk = require('./Talk');

module.exports.get_watch_next_by_id = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    
    // Controllo di sicurezza: verifica che l'ID sia nel percorso dell'URL
    if (!event.pathParameters || !event.pathParameters.id) {
        return { statusCode: 400, body: JSON.stringify({ message: 'ID del talk non fornito.' })};
    }
    
    const main_talk_id = event.pathParameters.id;

    try {
        await connect_to_db();
        const main_talk = await Talk.findById(main_talk_id);
        
        if (!main_talk) {
            console.warn(`ATTENZIONE: Talk con ID ${main_talk_id} non trovato nel database.`);
            return { statusCode: 404, body: JSON.stringify({ message: 'Talk non trovato.' }) };
        }

        const watch_next_ids = Array.isArray(main_talk.WatchNext_id) ? main_talk.WatchNext_id : [];

        let suggested_talks_details = [];
        if (watch_next_ids.length > 0) {
            console.log(`DEBUG: Cerco talk suggeriti con ID: ${JSON.stringify(watch_next_ids)}`);
            
            // Se ancora non trova, verifica che questi ID esistano davvero nel DB.
            suggested_talks_details = await Talk.find({
                '_id': { $in: watch_next_ids }
            });
            console.log(`INFO: Trovati ${suggested_talks_details.length} suggerimenti tramite WatchNext_id.`);
        }
        
        // Logica di fallback: se non troviamo suggerimenti tramite WatchNext_id
        if (suggested_talks_details.length === 0 && main_talk.tags && main_talk.tags.length > 0) {
            console.log("INFO: Nessun suggerimento trovato tramite WatchNext_id. Cerco suggerimenti basati sui tag.");
            suggested_talks_details = await Talk.find({
                tags: { $in: main_talk.tags },
                _id: { $ne: main_talk_id } 
            }).limit(3);
            console.log(`INFO: Trovati ${suggested_talks_details.length} suggerimenti tramite tag.`);
        }

        if (suggested_talks_details.length === 0) {
            console.log("INFO: Nessun suggerimento trovato. Restituisco [].");
            return { statusCode: 200, body: JSON.stringify([]) };
        }
        
        const main_talk_description_phrases = main_talk.description ? analyzeKeyPhrases(main_talk.description) : null;
        const main_talk_key_phrases = Array.isArray(main_talk_description_phrases) ? main_talk_description_phrases : [];

        const enriched_suggestions = suggested_talks_details.map(suggested_talk => {
            const suggested_talk_description_phrases = suggested_talk.comprehend_analysis ||
                (suggested_talk.description ? analyzeKeyPhrases(suggested_talk.description) : null);
            const suggested_talk_key_phrases = Array.isArray(suggested_talk_description_phrases) ? suggested_talk_description_phrases : [];
            
            const common_phrases = main_talk_key_phrases.filter(phrase => suggested_talk_key_phrases.includes(phrase));

            return {
                _id: suggested_talk._id,
                title: suggested_talk.title,
                url: suggested_talk.url,
                suggestion_reason: common_phrases
            };
        });

        console.log(`SUCCESSO: Trovati e arricchiti ${enriched_suggestions.length} suggerimenti.`);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(enriched_suggestions)
        };

    } catch (err) {
        console.error('ERRORE IMPREVISTO nel get_watch_next_by_id:', err);
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: 'Errore interno del server.', error: err.message })
        };
    }
};