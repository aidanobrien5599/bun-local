import { Hono } from "hono";
import { cors } from "hono/cors";
import mysql from "mysql2/promise";

const app = new Hono();

// Create MySQL connection pool
const pool = mysql.createPool({
  uri: Bun.env.MYSQL_URL,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});
console.log("=== DATABASE DEBUG ===");
console.log("MYSQL_URL:", Bun.env.MYSQL_URL);
console.log("URL length:", Bun.env.MYSQL_URL?.length);
console.log("URL type:", typeof Bun.env.MYSQL_URL);
console.log("=====================");

app.use("/*", cors());

app.get("/api/query", async (c) => {
  const apiKey = c.req.header("x-api-key");



  if (!apiKey || apiKey !== Bun.env.GET_API_KEY) {
    return c.json(
      {
        error: "Unauthorized",
        "received-key": apiKey,
        "expected-key": Bun.env.GET_API_KEY,
        "keys-match": apiKey === Bun.env.GET_API_KEY,
      },
      401
    );
  }

  try {
    const sql = `
    SELECT courses.course_id, courses.subject_code, courses.course_designation, courses.full_course_designation, courses.minimum_credits, courses.maximum_credits, courses.ethnic_studies, courses.social_science, courses.humanities, courses.biological_science, courses.physical_science, courses.mathematics, courses.computer_science, courses.engineering, courses.business, courses.art, courses.music, courses.theater, courses.dance, courses.other, courses.status, courses.created_at, courses.updated_at,
    sections.instructors, sections.status, sections.available_seats, sections.waitlist_total, sections.capacity, sections.enrolled, sections.meeting_time, sections.location, sections.instruction_mode, secionts.is_asynchronous
    FROM courses
    JOIN sections ON courses.course_id = sections.section_id
    LIMIT 10
    `;
    const [results] = await pool.execute(sql);
    return c.json({
      data: results,
      count: Array.isArray(results) ? results.length : 0,
    });
  } catch (error) {
    console.error("Database error:", error);
    return c.json(
      {
        error: "Database query failed",
        details: error instanceof Error ? error.message : "Unknown error",
      },
      500
    );
  }
});

// Health check endpoint
app.get("/health", async (c) => {
  try {
    await pool.execute("SELECT 1");
    return c.json({ status: "healthy", database: "connected" });
  } catch (error) {
    return c.json({ status: "unhealthy", database: "disconnected" }, 500);
  }
});

Bun.serve({
  port: Bun.env.PORT ?? 3000,
  fetch: app.fetch,
});