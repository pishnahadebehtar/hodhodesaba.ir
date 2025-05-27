import * as sdk from 'node-appwrite';
import axios from 'axios';
import { Query } from 'node-appwrite';

export default async function handler(context) {
  const logs = [];
  context.log('DEBUG: Function invoked');

  try {
   



    // Initialize Appwrite client
    const client = new sdk.Client()
      .setEndpoint(APPWRITE_ENDPOINT)
      .setProject(APPWRITE_PROJECT_ID)
      .setKey(APPWRITE_API_KEY);
    const databases = new sdk.Databases(client);
    context.log('DEBUG: Appwrite client initialized');

    // Football API config
    const baseUrl = 'https://api.football-data.org/v4';
    const headers = { 'X-Auth-Token': FOOTBALL_API_KEY };
    const maxRequests = 10;

    // Define tasks
    const tasks = [
      { endpoint: '/competitions/PL', priority: 1 },
      { endpoint: '/competitions/PL/standings', priority: 2 },
      { endpoint: '/competitions/PL/matches?status=FINISHED', priority: 3 },
      { endpoint: '/competitions/CL', priority: 1 },
      { endpoint: '/competitions/CL/standings', priority: 2 },
      { endpoint: '/competitions/CL/matches?status=FINISHED', priority: 3 },
      { endpoint: '/competitions/EC', priority: 1 },
      { endpoint: '/competitions/EC/standings', priority: 2 },
      { endpoint: '/competitions/EC/matches?status=FINISHED', priority: 3 },
    ];
    context.log(
      `DEBUG: Processing ${tasks.length} tasks: ${JSON.stringify(
        tasks.map((t) => ({ endpoint: t.endpoint })),
        null,
        2
      )}`
    );

    let requestsMade = 0;
    for (const task of tasks) {
      if (requestsMade >= maxRequests) {
        context.log('INFO: Max requests reached');
        break;
      }

      const endpoint = task.endpoint;
      context.log(`DEBUG: Processing task: ${endpoint}`);

      try {
        const response = await axios.get(`${baseUrl}${endpoint}`, { headers });
        requestsMade++;
        const data = response.data;
        context.log(
          `DEBUG: API response for ${endpoint}: ${JSON.stringify(data, null, 2).substring(0, 500)}...`
        );

        if (
          endpoint.startsWith('/competitions/') &&
          endpoint.endsWith('/standings')
        ) {
          const compCode = endpoint.split('/')[2];
          const standings = data.standings?.[0]?.table?.slice(0, 10) || [];
          context.log(
            `DEBUG: Processing ${standings.length} standings for ${compCode}`
          );
          for (const standing of standings) {
            await databases.createDocument(
              DATABASE_ID,
              STANDINGS_COLLECTION_ID,
              sdk.ID.unique(),
              {
                competition_code: compCode,
                position: standing.position,
                team_name: standing.team.name,
                points: standing.points,
                played_games: standing.playedGames,
              }
            );
            context.log(
              `DEBUG: Saved standing for ${compCode}: position ${standing.position}, team ${standing.team.name}`
            );
          }
        } else if (
          endpoint.startsWith('/competitions/') &&
          !endpoint.includes('/matches')
        ) {
          const compCode = endpoint.split('/')[2];
          const existingComps = await databases.listDocuments(
            DATABASE_ID,
            COMPETITIONS_COLLECTION_ID,
            [Query.equal('code', compCode)]
          );
          if (existingComps.documents.length === 0) {
            await databases.createDocument(
              DATABASE_ID,
              COMPETITIONS_COLLECTION_ID,
              sdk.ID.unique(),
              {
                code: compCode,
                name: data.name,
                area_name: data.area.name,
              },
              [sdk.Permission.read(sdk.Role.any())]
            );
            context.log(`DEBUG: Saved competition ${compCode}: ${data.name}`);
          } else {
            context.log(`DEBUG: Skipped existing competition ${compCode}`);
          }
        } else if (endpoint.includes('/matches')) {
          const compCode = endpoint.split('/')[2];
          const matches = data.matches?.slice(0, 10) || [];
          context.log(
            `DEBUG: Processing ${matches.length} matches for ${compCode}`
          );
          for (const match of matches) {
            if (match.status === 'FINISHED') {
              const existingMatches = await databases.listDocuments(
                DATABASE_ID,
                MATCHES_COLLECTION_ID,
                [Query.equal('match_id', match.id)]
              );
              if (existingMatches.documents.length === 0) {
                const documentId = sdk.ID.unique();
                await databases.createDocument(
                  DATABASE_ID,
                  MATCHES_COLLECTION_ID,
                  documentId,
                  {
                    $id: documentId, // Include the Appwrite document ID
                    match_id: match.id,
                    competition_code: compCode,
                    home_team_name: match.homeTeam.name,
                    away_team_name: match.awayTeam.name,
                    score_home: match.score.fullTime.home ?? null,
                    score_away: match.score.fullTime.away ?? null,
                    status: match.status,
                    utc_date: match.utcDate,
                  },
                  [sdk.Permission.read(sdk.Role.any())]
                );
                context.log(
                  `DEBUG: Saved match ${match.id} for ${compCode} with document ID ${documentId}`
                );
              } else {
                context.log(
                  `DEBUG: Skipped existing match ${match.id} for ${compCode}`
                );
              }
            }
          }
        }
      } catch (error) {
        context.log(`ERROR: Failed task ${endpoint}: ${error.message}`);
      }
    }

    return context.res.json({
      status: 'success',
      logs,
      requests_made: requestsMade,
    });
  } catch (error) {
    context.log(`ERROR: Unexpected error: ${error.message}`);
    return context.res.json({ status: 'error', message: error.message, logs });
  }
}
