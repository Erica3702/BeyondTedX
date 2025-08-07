'use strict';

const connect_to_db = require('./db');
const User = require('./User');
const bcrypt = require('bcryptjs');

module.exports.registerUser = async (event) => {
    try {
        const data = JSON.parse(event.body);
        const { username, password, userType } = data; 

        if (!username || !password) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Username e password sono campi obbligatori." })
            };
        }

        await connect_to_db();
        
        const existingUser = await User.findOne({ username: username.toLowerCase() });
        if (existingUser) {
            return {
                statusCode: 409,
                body: JSON.stringify({ message: "Questo username è già stato registrato." })
            };
        }
        
        const hashedPassword = await bcrypt.hash(password, 12);

        const newUser = new User({
            username,
            password: hashedPassword,
            userType: userType 
        });

        await newUser.save();
        
        return {
            statusCode: 201,
            body: JSON.stringify({ message: "Utente registrato con successo!" })
        };

    } catch (error) {
        console.error("Errore imprevisto durante la registrazione:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Impossibile completare la registrazione.", error: error.message })
        };
    }
};