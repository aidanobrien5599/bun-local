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
    // STEP 1: Get query parameters (including new RMP parameters)
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
      literature,
      min_cumulative_gpa,
      min_most_recent_gpa,
      // NEW: RMP-related parameters
      min_instructor_rating,
      min_difficulty_rating,
      min_num_ratings
    } = c.req.query();

    // STEP 2: Determine if we need to filter by RMP data
    const hasRMPFilters = !!(
      min_instructor_rating || 
      max_instructor_rating || 
      min_difficulty_rating || 
      max_difficulty_rating ||
      min_num_ratings
    );

    // STEP 3: Build WHERE clause filters
    let sectionFilters = [];
    let courseFilters = [];
    let rmpFilters = [];
    let filterParams = [];
    
    // Section filters (existing code)
    if (status) {
      const statusList = status.split(',').map(s => s.trim().toUpperCase());
      if (statusList.length === 1) {
        sectionFilters.push("sections.status = ?");
        filterParams.push(statusList[0]);
      } else {
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

    // Course filters (existing code)
    if (min_credits) {
      courseFilters.push("courses.minimum_credits >= ?");
      filterParams.push(min_credits);
    }
    if (max_credits) {
      courseFilters.push("courses.maximum_credits <= ?");
      filterParams.push(max_credits);
    }
    if (level) {
      courseFilters.push("courses.level = ?");
      filterParams.push(level);
    }
    if (ethnic_studies) {
      courseFilters.push("courses.ethnic_studies = ?");
      filterParams.push('ETHNIC ST');
    }
    if (social_science) {
      courseFilters.push("courses.social_science = ?");
      filterParams.push('S');
    }
    if (humanities) {
      courseFilters.push("courses.humanities = ?");
      filterParams.push('H');
    }
    if (biological_science) {
      courseFilters.push("courses.biological_science = ?");
      filterParams.push('B');
    }
    if (physical_science) {
      courseFilters.push("courses.physical_science = ?");
      filterParams.push('P');
    }
    if (natural_science) {
      courseFilters.push("courses.natural_science = ?");
      filterParams.push('N');
    }
    if (literature) {
      courseFilters.push("courses.literature = ?");
      filterParams.push('L');
    }
    if (min_cumulative_gpa) {
      courseFilters.push("madgrades_course_grades.cumulative_gpa >= ?");
      filterParams.push(min_cumulative_gpa);
    }
    if (min_most_recent_gpa) {
      courseFilters.push("madgrades_course_grades.most_recent_gpa >= ?");
      filterParams.push(min_most_recent_gpa);
    }

    // NEW: RMP filters
    if (min_instructor_rating) {
      rmpFilters.push("rmp_cleaned.avg_rating >= ?");
      filterParams.push(parseFloat(min_instructor_rating));
    }

    if (min_difficulty_rating) {
      rmpFilters.push("rmp_cleaned.avg_difficulty >= ?");
      filterParams.push(parseFloat(min_difficulty_rating));
    }

    if (min_num_ratings) {
      rmpFilters.push("rmp_cleaned.num_ratings >= ?");
      filterParams.push(parseInt(min_num_ratings));
    }

    // STEP 4: Combine all filters and build WHERE clause
    let allFilters = [...sectionFilters, ...courseFilters, ...rmpFilters];
    const whereClause = allFilters.length > 0 
      ? `WHERE ${allFilters.join(' AND ')}` 
      : '';

    console.log("WHERE CLAUSE:", whereClause);
    console.log("Has RMP Filters:", hasRMPFilters);

    // STEP 5: Determine JOIN type for RMP table
    const rmpJoinType = hasRMPFilters ? 'JOIN' : 'LEFT JOIN';

    // STEP 6: Build and execute distinct courses query
    let distinctCoursesSql, queryParams;
    const limitValue = parseInt(limit) || 10;
    
    if (allFilters.length > 0) {
      distinctCoursesSql = `
        SELECT DISTINCT courses.course_id
        FROM courses
        JOIN sections ON courses.course_id = sections.course_id
        LEFT JOIN madgrades_course_grades ON courses.course_designation = madgrades_course_grades.course_name
        ${rmpJoinType} rmp_cleaned ON FIND_IN_SET(TRIM(rmp_cleaned.full_name), REPLACE(sections.instructors, ', ', ',')) > 0
        ${whereClause}
        LIMIT ${limitValue}
      `;
      queryParams = filterParams;
    } else {
      // No filters - always use LEFT JOIN to include all courses
      distinctCoursesSql = `
        SELECT DISTINCT courses.course_id
        FROM courses
        JOIN sections ON courses.course_id = sections.course_id
        LEFT JOIN rmp_cleaned ON FIND_IN_SET(TRIM(rmp_cleaned.full_name), REPLACE(sections.instructors, ', ', ',')) > 0
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

    // STEP 7: Get full course details with RMP data
    const courseIdList = courseIds.map(row => row.course_id);
    const coursePlaceholders = courseIdList.map(() => '?').join(',');
    
    const coursesSql = `
      SELECT 
        courses.course_id, 
        courses.subject_code, 
        courses.course_designation, 
        courses.full_course_designation, 
        courses.minimum_credits, 
        courses.maximum_credits, 
        courses.ethnic_studies, 
        courses.social_science, 
        courses.humanities, 
        courses.biological_science, 
        courses.physical_science, 
        courses.natural_science, 
        courses.literature, 
        courses.level,
        madgrades_course_grades.cumulative_gpa, 
        madgrades_course_grades.most_recent_gpa,
        AVG(rmp_cleaned.avg_rating) as avg_instructor_rating,
        AVG(rmp_cleaned.avg_difficulty) as avg_difficulty_rating,
        COUNT(DISTINCT rmp_cleaned.id) as rated_instructors_count
      FROM courses 
      LEFT JOIN madgrades_course_grades ON courses.course_designation = madgrades_course_grades.course_name
      LEFT JOIN sections ON courses.course_id = sections.course_id
      ${rmpJoinType} rmp_cleaned ON FIND_IN_SET(TRIM(rmp_cleaned.full_name), REPLACE(sections.instructors, ', ', ',')) > 0
      WHERE courses.course_id IN (${coursePlaceholders})
      GROUP BY courses.course_id, courses.subject_code, courses.course_designation, 
               courses.full_course_designation, courses.minimum_credits, courses.maximum_credits,
               courses.ethnic_studies, courses.social_science, courses.humanities, 
               courses.biological_science, courses.physical_science, courses.natural_science,
               courses.literature, courses.level, madgrades_course_grades.cumulative_gpa,
               madgrades_course_grades.most_recent_gpa
      ORDER BY courses.course_id
    `;
    
    const [coursesResults] = await pool.execute(coursesSql, courseIdList);
    
    // STEP 8: Get sections for these courses with conditional filtering
    let sectionsSql, sectionsParams;
    
    if (sectionFilters.length > 0 || hasRMPFilters) {
      sectionsSql = `
        SELECT 
          sections.course_id, 
          sections.instructors, 
          sections.status, 
          sections.available_seats, 
          sections.waitlist_total, 
          sections.capacity, 
          sections.enrolled, 
          sections.meeting_time, 
          sections.location, 
          sections.instruction_mode, 
          sections.is_asynchronous,
          GROUP_CONCAT(
            DISTINCT CONCAT(
              COALESCE(rmp_cleaned.full_name, ''), ':', 
              COALESCE(rmp_cleaned.avg_rating, 'N/A'), ':', 
              COALESCE(rmp_cleaned.avg_difficulty, 'N/A'), ':',
              COALESCE(rmp_cleaned.num_ratings, 'N/A')
            ) SEPARATOR ';'
          ) as instructor_ratings
        FROM sections 
        ${rmpJoinType} rmp_cleaned ON FIND_IN_SET(TRIM(rmp_cleaned.full_name), REPLACE(sections.instructors, ', ', ',')) > 0
        WHERE sections.course_id IN (${coursePlaceholders}) 
        ${sectionFilters.length > 0 ? `AND ${sectionFilters.join(' AND ')}` : ''}
        ${rmpFilters.length > 0 ? `AND ${rmpFilters.join(' AND ')}` : ''}
        GROUP BY sections.course_id, sections.instructors, sections.status, sections.available_seats,
                 sections.waitlist_total, sections.capacity, sections.enrolled, sections.meeting_time,
                 sections.location, sections.instruction_mode, sections.is_asynchronous
        ORDER BY sections.course_id, sections.status DESC
      `;
      
      // Build parameters for sections query
      let sectionFilterParams = [];
      for (let i = 0; i < sectionFilters.length; i++) {
        sectionFilterParams.push(filterParams[i]);
      }
      let rmpFilterParams = [];
      for (let i = 0; i < rmpFilters.length; i++) {
        rmpFilterParams.push(filterParams[sectionFilters.length + courseFilters.length + i]);
      }
      sectionsParams = [...courseIdList, ...sectionFilterParams, ...rmpFilterParams];
    } else {
      // No section or RMP filters - use LEFT JOIN to include all sections
      sectionsSql = `
        SELECT 
          sections.course_id, 
          sections.instructors, 
          sections.status, 
          sections.available_seats, 
          sections.waitlist_total, 
          sections.capacity, 
          sections.enrolled, 
          sections.meeting_time, 
          sections.location, 
          sections.instruction_mode, 
          sections.is_asynchronous,
          GROUP_CONCAT(
            DISTINCT CONCAT(
              COALESCE(rmp_cleaned.full_name, ''), ':', 
              COALESCE(rmp_cleaned.avg_rating, 'N/A'), ':', 
              COALESCE(rmp_cleaned.avg_difficulty, 'N/A'), ':',
              COALESCE(rmp_cleaned.num_ratings, 'N/A')
            ) SEPARATOR ';'
          ) as instructor_ratings
        FROM sections 
        LEFT JOIN rmp_cleaned ON FIND_IN_SET(TRIM(rmp_cleaned.full_name), REPLACE(sections.instructors, ', ', ',')) > 0
        WHERE sections.course_id IN (${coursePlaceholders})
        GROUP BY sections.course_id, sections.instructors, sections.status, sections.available_seats,
                 sections.waitlist_total, sections.capacity, sections.enrolled, sections.meeting_time,
                 sections.location, sections.instruction_mode, sections.is_asynchronous
        ORDER BY sections.course_id, sections.status DESC
      `;
      sectionsParams = courseIdList;
    }
    
    const [sectionsResults] = await pool.execute(sectionsSql, sectionsParams);
    
    // STEP 9: Group sections by course_id and parse instructor ratings
    const sectionsByCourse = {};
    if (Array.isArray(sectionsResults)) {
      sectionsResults.forEach(section => {
        if (!sectionsByCourse[section.course_id]) {
          sectionsByCourse[section.course_id] = [];
        }
        
        // Parse instructor ratings if they exist
        let instructorRatingsData = null;
        if (section.instructor_ratings) {
          instructorRatingsData = section.instructor_ratings.split(';')
            .filter(rating => rating && !rating.startsWith(':')) // Filter out empty ratings
            .map(rating => {
              const [name, overall, difficulty, numRatings] = rating.split(':');
              return {
                name: name || null,
                avg_rating: overall !== 'N/A' && overall ? parseFloat(overall) : null,
                avg_difficulty: difficulty !== 'N/A' && difficulty ? parseFloat(difficulty) : null,
                num_ratings: numRatings !== 'N/A' && numRatings ? parseInt(numRatings) : null
              };
            })
            .filter(rating => rating.name); // Only include ratings with actual instructor names
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
          instructor_ratings: instructorRatingsData
        });
      });
    }
    
    // STEP 10: Combine courses with their sections
    const coursesWithSections = coursesResults.map(course => ({
      ...course,
      sections: sectionsByCourse[course.course_id] || []
    }));

    // STEP 11: Return response with filter information
    return c.json({
      data: coursesWithSections,
      count: coursesWithSections.length,
      filters_applied: {
        status,
        instructor,
        min_available_seats,
        instruction_mode,
        min_instructor_rating,
        min_difficulty_rating,
        min_num_ratings,
        rmp_filtering_active: hasRMPFilters
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