'use-strict';

const connect_to_db = require('./db');
const User = require('./User');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

module.exports.loginUser = async (event) => {
    try {
        const { username, password } = JSON.parse(event.body);

        if (!username || !password) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Username e password sono campi obbligatori." })
            };
        }

        await connect_to_db();
        console.log('Ricerca utente nel database...');

        const user = await User.findOne({ username: username.toLowerCase() });
        if (!user) {
            console.log('Tentativo di login fallito: utente non trovato.');
            return {
                statusCode: 401,
                body: JSON.stringify({ message: "Le credenziali fornite non sono corrette." })
            };
        }

        console.log('Utente trovato. Confronto delle password...');
        const isPasswordCorrect = await bcrypt.compare(password, user.password);

        if (!isPasswordCorrect) {
            console.log('Tentativo di login fallito: password errata.');
            return {
                statusCode: 401,
                body: JSON.stringify({ message: "Le credenziali fornite non sono corrette." })
            };
        }
        
        console.log('Login riuscito. Generazione del token JWT...');
        const token = jwt.sign(
            { 
                userId: user._id, 
                username: user.username,
                userType: user.userType
            },
            process.env.JWT_SECRET, 
            { expiresIn: '3h' } // Token valido per 3 ore
        );

        return {
            statusCode: 200,
            body: JSON.stringify({ 
                message: "Autenticazione avvenuta con successo.", 
                token: token 
            })
        };

    } catch (error) {
        console.error("Errore imprevisto durante il login:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Impossibile completare il login.", error: error.message })
        };
    }
};