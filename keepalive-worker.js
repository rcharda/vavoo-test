// Cloudflare Worker — ping périodique pour empêcher Render de s'endormir.
// Configuration : ajoute un "Cron Trigger" toutes les 10 minutes dans le
// tableau de bord Cloudflare (Workers > ton worker > Triggers > Cron Triggers).

const TARGET_URL = 'https://vavoo-test.onrender.com/';

export default {
    async scheduled(event, env, ctx) {
        try {
            const response = await fetch(TARGET_URL, { method: 'GET' });
            console.log(`Ping ${TARGET_URL} -> ${response.status}`);
        } catch (error) {
            console.log(`Ping failed: ${error.message}`);
        }
    },

    // Permet aussi de tester manuellement en visitant l'URL du worker
    async fetch(request, env, ctx) {
        try {
            const response = await fetch(TARGET_URL, { method: 'GET' });
            return new Response(`Pinged ${TARGET_URL}: ${response.status}`, { status: 200 });
        } catch (error) {
            return new Response(`Ping failed: ${error.message}`, { status: 500 });
        }
    }
};
