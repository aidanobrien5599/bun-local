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
    // Get query parameters for filtering
    const { 
      status, 
      instructor, 
      min_available_seats, 
      instruction_mode,
      limit = 10,
      min_credits,
      max_credits,
      level,
      ethnic_studies,
      social_science,
      humanities,
      biological_science,
      physical_science,
      natural_science,
      literature
    } = c.req.query();

    // Build WHERE clause for section filters
    let sectionFilters = [];
    let courseFilters = [];
    let filterParams = [];
    
    if (status) {
      // Handle multiple statuses separated by commas
      const statusList = status.split(',').map(s => s.trim().toUpperCase());
      if (statusList.length === 1) {
        sectionFilters.push("sections.status = ?");
        filterParams.push(statusList[0]);
      } else {
        // Multiple statuses - use IN clause
        const statusPlaceholders = statusList.map(() => '?').join(',');
        sectionFilters.push(`sections.status IN (${statusPlaceholders})`);
        filterParams.push(...statusList);
      }
    }
    if (instructor) {
      sectionFilters.push("sections.instructors LIKE ?");
      filterParams.push(`%${instructor}%`);
    }
    if (instruction_mode) {
      sectionFilters.push("sections.instruction_mode = ?");
      filterParams.push(instruction_mode);
    }

    if (min_credits) {
      courseFilters.push("courses.minimum_credits >= ?");
      filterParams.push(min_credits);
    }
    if (max_credits) {
      courseFilters.push("courses.maximum_credits <= ?");
      filterParams.push(max_credits);
    }

    if (level){
      courseFilters.push("courses.level = ?");
      filterParams.push(level);
    }
    if(ethnic_studies){
      courseFilters.push("courses.ethnic_studies = ?");
      filterParams.push('ETHNIC ST');
    }
    if(social_science){
      courseFilters.push("courses.social_science = ?");
      filterParams.push('S');
    }
    if(humanities){
      courseFilters.push("courses.humanities = ?");
      filterParams.push('H');
    }
    if(biological_science){
      courseFilters.push("courses.biological_science = ?");
      filterParams.push('B');
    }
    if(physical_science){
      courseFilters.push("courses.physical_science = ?");
      filterParams.push('P');
    }
    if(natural_science){
      courseFilters.push("courses.natural_science = ?");
      filterParams.push('N');
    }
    if(literature){
      courseFilters.push("courses.literature = ?");
      filterParams.push('L');
    }

    let allFilters = [...sectionFilters, ...courseFilters];
    const whereClause = allFilters.length > 0 
      ? `WHERE ${allFilters.join(' AND ')}` 
      : '';

    console.log("WHERE CLAUSE:", whereClause);  

    // Build the complete SQL with proper parameter handling
    let distinctCoursesSql, queryParams;
    const limitValue = parseInt(limit) || 10; // Ensure it's a valid integer
    
    if (allFilters.length > 0) {
      // With filters - use parameterized query for filters, direct substitution for LIMIT
      distinctCoursesSql = `
        SELECT DISTINCT courses.course_id
        FROM courses
        JOIN sections ON courses.course_id = sections.course_id
        ${whereClause}
        LIMIT ${limitValue}
      `;
      console.log("DISTINCT COURSES SQL:", distinctCoursesSql);
      queryParams = filterParams;
    } else {
      // No filters - simple query with direct LIMIT
      distinctCoursesSql = `
        SELECT DISTINCT courses.course_id
        FROM courses
        JOIN sections ON courses.course_id = sections.course_id
        LIMIT ${limitValue}
      `;
      queryParams = [];
    }
    
    const [courseIds] = await pool.execute(distinctCoursesSql, queryParams);
    
    console.log("=== QUERY DEBUG ===");
    console.log("SQL:", distinctCoursesSql);
    console.log("Params:", queryParams);
    console.log("Params length:", queryParams.length);
    console.log("==================");
    
    if (!Array.isArray(courseIds) || courseIds.length === 0) {
      return c.json({
        data: [],
        count: 0,
      });
    }

    // Get full course details for these course IDs
    const courseIdList = courseIds.map(row => row.course_id);
    const coursePlaceholders = courseIdList.map(() => '?').join(',');
    
    const coursesSql = `
      SELECT course_id, subject_code, course_designation, full_course_designation, 
             minimum_credits, maximum_credits, ethnic_studies, social_science, 
             humanities, biological_science, physical_science, natural_science, 
             literature, level
      FROM courses 
      WHERE course_id IN (${coursePlaceholders})
      ORDER BY course_id
    `;
    
    const [coursesResults] = await pool.execute(coursesSql, courseIdList);
    
    // Get sections for these courses - apply same filters if any exist
    let sectionsSql, sectionsParams;
    
    if (sectionFilters.length > 0) {
      // Apply the same filters to sections query
      sectionsSql = `
        SELECT course_id, instructors, status, available_seats, waitlist_total, 
               capacity, enrolled, meeting_time, location, instruction_mode, is_asynchronous
        FROM sections 
        WHERE course_id IN (${coursePlaceholders}) AND ${sectionFilters.join(' AND ')}
        ORDER BY course_id, status DESC
      `;
      // Only pass section filter parameters (course filters already applied in first query)
      const sectionFilterParams = filterParams.slice(0, sectionFilters.length);
      sectionsParams = [...courseIdList, ...sectionFilterParams];
    } else {
      // No filters - get all sections
      sectionsSql = `
        SELECT course_id, instructors, status, available_seats, waitlist_total, 
               capacity, enrolled, meeting_time, location, instruction_mode, is_asynchronous
        FROM sections 
        WHERE course_id IN (${coursePlaceholders})
        ORDER BY course_id, status DESC
      `;
      sectionsParams = courseIdList;
    }
    
    const [sectionsResults] = await pool.execute(sectionsSql, sectionsParams);
    
    // Group sections by course_id
    const sectionsByCourse = {};
    if (Array.isArray(sectionsResults)) {
      sectionsResults.forEach(section => {
        if (!sectionsByCourse[section.course_id]) {
          sectionsByCourse[section.course_id] = [];
        }
        sectionsByCourse[section.course_id].push({
          instructors: section.instructors,
          status: section.status,
          available_seats: section.available_seats,
          waitlist_total: section.waitlist_total,
          capacity: section.capacity,
          enrolled: section.enrolled,
          meeting_time: section.meeting_time,
          location: section.location,
          instruction_mode: section.instruction_mode,
          is_asynchronous: section.is_asynchronous,
        });
      });
    }
    
    // Combine courses with their sections
    const coursesWithSections = coursesResults.map(course => ({
      ...course,
      sections: sectionsByCourse[course.course_id] || []
    }));

    return c.json({
      data: coursesWithSections,
      count: coursesWithSections.length,
      filters_applied: {
        status,
        instructor,
        min_available_seats,
        instruction_mode
      }
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