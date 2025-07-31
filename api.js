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
      search_param,
      status,
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
      // New RMP section-level filters
      min_section_avg_rating,
      min_section_avg_difficulty,
      min_section_total_ratings,
      min_section_avg_would_take_again,
      page = 1
    } = c.req.query();

    // Build WHERE clause for section filters
    let sectionFilters = [];
    let courseFilters = [];
    let rmpSectionFilters = []; // New: for RMP section-level filters
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

    if (instruction_mode) {
      sectionFilters.push("sections.instruction_mode = ?");
      filterParams.push(instruction_mode);
    }

    if (min_available_seats) {
      sectionFilters.push("sections.available_seats >= ?");
      filterParams.push(min_available_seats);
    }

    // Course-level filters
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

    // New RMP section-level filters
    if (min_section_avg_rating) {
      rmpSectionFilters.push("section_rmp_avg.section_avg_rating >= ?");
      filterParams.push(parseFloat(min_section_avg_rating));
    }
    if (min_section_avg_difficulty) {
      rmpSectionFilters.push("section_rmp_avg.section_avg_difficulty >= ?");
      filterParams.push(parseFloat(min_section_avg_difficulty));
    }
    if (min_section_total_ratings) {
      rmpSectionFilters.push("section_rmp_avg.section_total_ratings >= ?");
      filterParams.push(parseInt(min_section_total_ratings));
    }
    if (min_section_avg_would_take_again) {
      rmpSectionFilters.push("section_rmp_avg.section_avg_would_take_again >= ?");
      filterParams.push(parseFloat(min_section_avg_would_take_again));
    }

    

    if (search_param) {
      courseFilters.push("(courses.course_designation LIKE ? OR courses.full_course_designation LIKE ? OR si.instructor_name LIKE ?)");
      const searchValue = `%${search_param}%`;
      filterParams.push(searchValue, searchValue, searchValue);
    }

    const offset = (page - 1) * limit;

    let allFilters = [...sectionFilters, ...courseFilters, ...rmpSectionFilters];
    
    console.log("All filters:", allFilters);
    console.log("Filter params:", filterParams);

    const limitValue = parseInt(limit) || 10;


      let totalCountSql = `
     WITH section_rmp_avg AS (
          SELECT 
            sections.section_id,
            sections.course_id,
            CASE 
              WHEN COUNT(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL THEN 1 END) > 0 
              THEN ROUND(
                SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
                    THEN rmp_cleaned.avg_rating * COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END) / 
                SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
                    THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END), 2)
              ELSE NULL 
            END as section_avg_rating,
            CASE 
              WHEN COUNT(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL THEN 1 END) > 0 
              THEN ROUND(
                SUM(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL 
                    THEN rmp_cleaned.avg_difficulty * COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END) / 
                SUM(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL 
                    THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END), 2)
              ELSE NULL 
            END as section_avg_difficulty,
            SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
                THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                ELSE 0 END) as section_total_ratings,
            CASE 
              WHEN COUNT(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL THEN 1 END) > 0 
              THEN ROUND(
                SUM(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL 
                    THEN rmp_cleaned.would_take_again_percent * COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END) / 
                SUM(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL 
                    THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END), 2)
              ELSE NULL 
            END as section_avg_would_take_again
          FROM sections
          LEFT JOIN section_instructors si ON sections.section_id = si.section_id
          LEFT JOIN rmp_cleaned ON si.instructor_name = rmp_cleaned.full_name
          GROUP BY sections.section_id, sections.course_id
        )
    SELECT COUNT(DISTINCT courses.course_id) AS total
    FROM courses
    JOIN sections ON courses.course_id = sections.course_id
    JOIN madgrades_course_grades ON courses.course_designation = madgrades_course_grades.course_name
    LEFT JOIN section_instructors si ON sections.section_id = si.section_id
    ${rmpSectionFilters.length > 0 ? 'JOIN section_rmp_avg ON sections.section_id = section_rmp_avg.section_id' : ''}
    ${allFilters.length > 0 ? `WHERE ${allFilters.join(' AND ')}` : ''}
  `;

    const [countRows] = await pool.execute(totalCountSql, filterParams);
    const totalCount = countRows?.[0]?.total ?? 0;
    const hasMore = offset + limitValue < totalCount;

    // Build the query with RMP section calculations
    let distinctCoursesSql, queryParams;
  

    

    if (allFilters.length > 0) {
      // Create a CTE (Common Table Expression) to calculate section-level RMP averages
      distinctCoursesSql = `
        WITH section_rmp_avg AS (
          SELECT 
            sections.section_id,
            sections.course_id,
            CASE 
              WHEN COUNT(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL THEN 1 END) > 0 
              THEN ROUND(
                SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
                    THEN rmp_cleaned.avg_rating * COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END) / 
                SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
                    THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END), 2)
              ELSE NULL 
            END as section_avg_rating,
            CASE 
              WHEN COUNT(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL THEN 1 END) > 0 
              THEN ROUND(
                SUM(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL 
                    THEN rmp_cleaned.avg_difficulty * COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END) / 
                SUM(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL 
                    THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END), 2)
              ELSE NULL 
            END as section_avg_difficulty,
            SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
                THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                ELSE 0 END) as section_total_ratings,
            CASE 
              WHEN COUNT(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL THEN 1 END) > 0 
              THEN ROUND(
                SUM(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL 
                    THEN rmp_cleaned.would_take_again_percent * COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END) / 
                SUM(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL 
                    THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                    ELSE 0 END), 2)
              ELSE NULL 
            END as section_avg_would_take_again
          FROM sections
          LEFT JOIN section_instructors si ON sections.section_id = si.section_id
          LEFT JOIN rmp_cleaned ON si.instructor_name = rmp_cleaned.full_name
          GROUP BY sections.section_id, sections.course_id
        )
        SELECT DISTINCT courses.course_id
        FROM courses
        JOIN sections ON courses.course_id = sections.course_id
        JOIN madgrades_course_grades ON courses.course_designation = madgrades_course_grades.course_name
        LEFT JOIN section_instructors si ON sections.section_id = si.section_id
        ${rmpSectionFilters.length > 0 ? 'JOIN section_rmp_avg ON sections.section_id = section_rmp_avg.section_id' : ''}
        ${allFilters.length > 0 ? `WHERE ${allFilters.join(' AND ')}` : ''}
        LIMIT ${limitValue} OFFSET ${offset}
      `;
      queryParams = filterParams;
    } else {
      // No filters - simple query
      distinctCoursesSql = `
        SELECT DISTINCT courses.course_id
        FROM courses
        JOIN sections ON courses.course_id = sections.course_id
        LIMIT ${limitValue} OFFSET ${offset}
      `;
      queryParams = [];
    }

    console.log("=== DISTINCT COURSES QUERY ===");
    console.log("SQL:", distinctCoursesSql);
    console.log("Params:", queryParams);
    console.log("===============================");

    const [courseIds] = await pool.execute(distinctCoursesSql, queryParams);

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
             literature, level, 
               CAST(madgrades_course_grades.cumulative_gpa AS FLOAT) AS cumulative_gpa,
              CAST(madgrades_course_grades.most_recent_gpa AS FLOAT) AS most_recent_gpa,
              CAST(madgrades_course_grades.most_recent_gpa AS FLOAT) AS most_recent_gpa,
              madgrades_course_grades.course_uuid AS madgrades_course_uuid

      FROM courses 
      JOIN madgrades_course_grades ON courses.course_designation = madgrades_course_grades.course_name
      WHERE course_id IN (${coursePlaceholders})
      ORDER BY course_id
    `;

    const [coursesResults] = await pool.execute(coursesSql, courseIdList);

    // Get sections with pre-calculated RMP averages
    const sectionsWithRmpSql = `
      WITH section_rmp_avg AS (
        SELECT 
          sections.section_id,
          sections.course_id,
          sections.status,
          sections.available_seats,
          sections.waitlist_total,
          sections.capacity,
          sections.enrolled,
          sections.meeting_time,
          sections.location,
          sections.instruction_mode,
          sections.is_asynchronous,
          CASE 
            WHEN COUNT(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL THEN 1 END) > 0 
            THEN ROUND(
              SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
                  THEN rmp_cleaned.avg_rating * COALESCE(rmp_cleaned.num_ratings, 1) 
                  ELSE 0 END) / 
              SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
                  THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                  ELSE 0 END), 2)
            ELSE NULL 
          END as section_avg_rating,
          CASE 
            WHEN COUNT(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL THEN 1 END) > 0 
            THEN ROUND(
              SUM(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL 
                  THEN rmp_cleaned.avg_difficulty * COALESCE(rmp_cleaned.num_ratings, 1) 
                  ELSE 0 END) / 
              SUM(CASE WHEN rmp_cleaned.avg_difficulty IS NOT NULL 
                  THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                  ELSE 0 END), 2)
            ELSE NULL 
          END as section_avg_difficulty,
          CAST(SUM(CASE WHEN rmp_cleaned.avg_rating IS NOT NULL 
            THEN COALESCE(rmp_cleaned.num_ratings, 1) 
            ELSE 0 END) AS UNSIGNED) AS section_total_ratings,
          CASE 
            WHEN COUNT(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL THEN 1 END) > 0 
            THEN ROUND(
              SUM(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL 
                  THEN rmp_cleaned.would_take_again_percent * COALESCE(rmp_cleaned.num_ratings, 1) 
                  ELSE 0 END) / 
              SUM(CASE WHEN rmp_cleaned.would_take_again_percent IS NOT NULL 
                  THEN COALESCE(rmp_cleaned.num_ratings, 1) 
                  ELSE 0 END), 2)
            ELSE NULL 
          END as section_avg_would_take_again
        FROM sections
        LEFT JOIN section_instructors si ON sections.section_id = si.section_id
        LEFT JOIN rmp_cleaned ON si.instructor_name = rmp_cleaned.full_name
        WHERE sections.course_id IN (${coursePlaceholders})
        GROUP BY sections.section_id, sections.course_id, sections.status, 
                 sections.available_seats, sections.waitlist_total, sections.capacity, 
                 sections.enrolled, sections.meeting_time, sections.location, 
                 sections.instruction_mode, sections.is_asynchronous
      )
      SELECT 
        sra.*,
        si.instructor_name,
        rmp.avg_rating as instructor_avg_rating,
        rmp.avg_difficulty as instructor_avg_difficulty,
        rmp.num_ratings as instructor_num_ratings,
        rmp.would_take_again_percent as instructor_would_take_again_percent
      FROM section_rmp_avg sra
      LEFT JOIN section_instructors si ON sra.section_id = si.section_id
      LEFT JOIN rmp_cleaned rmp ON si.instructor_name = rmp.full_name
      ORDER BY sra.course_id, sra.status DESC, si.instructor_name
    `;

    const [sectionsResults] = await pool.execute(sectionsWithRmpSql, courseIdList);

    console.log("=== SECTIONS WITH RMP RESULTS ===");
    console.log("Total sections returned:", sectionsResults.length);
    console.log("First few sections:", sectionsResults.slice(0, 3));
    console.log("==================================");

    // Group sections by course and section
    const sectionsByCourse = {};
    
    if (Array.isArray(sectionsResults)) {
      sectionsResults.forEach((row) => {
        if (!sectionsByCourse[row.course_id]) {
          sectionsByCourse[row.course_id] = {};
        }

        // Use section_id as key to group instructors by section
        if (!sectionsByCourse[row.course_id][row.section_id]) {
          sectionsByCourse[row.course_id][row.section_id] = {
            section_id: row.section_id,
            status: row.status,
            available_seats: row.available_seats,
            waitlist_total: row.waitlist_total,
            capacity: row.capacity,
            enrolled: row.enrolled,
            meeting_time: row.meeting_time,
            location: row.location,
            instruction_mode: row.instruction_mode,
            is_asynchronous: row.is_asynchronous,
            instructors: [],
            // Pre-calculated section-level averages from SQL
            section_avg_rating: row.section_avg_rating,
            section_avg_difficulty: row.section_avg_difficulty,
            section_total_ratings: row.section_total_ratings,
            section_avg_would_take_again: row.section_avg_would_take_again
          };
        }

        // Add instructor if it exists and isn't already added
        if (row.instructor_name) {
          const existingInstructor = sectionsByCourse[row.course_id][row.section_id].instructors
            .find(inst => inst.name === row.instructor_name);

          if (!existingInstructor) {
            const instructorData = {
              name: row.instructor_name,
              avg_rating: row.instructor_avg_rating,
              avg_difficulty: row.instructor_avg_difficulty,
              num_ratings: row.instructor_num_ratings,
              would_take_again_percent: row.instructor_would_take_again_percent
            };

            sectionsByCourse[row.course_id][row.section_id].instructors.push(instructorData);
          }
        }
      });
    }

    // Convert sections object to array for each course
    Object.keys(sectionsByCourse).forEach(courseId => {
      sectionsByCourse[courseId] = Object.values(sectionsByCourse[courseId]);
    });

    // Combine courses with their sections
    const coursesWithSections = coursesResults.map(course => {
      return {
        ...course,
        sections: sectionsByCourse[course.course_id] || []
      };
    });

    console.log("=== FINAL COURSES WITH SECTIONS ===");
    console.log(`Total courses: ${coursesWithSections.length}`);
    coursesWithSections.forEach(course => {
      console.log(`Course ${course.course_id}: ${course.sections.length} sections`);
      course.sections.forEach(section => {
        console.log(`  Section ${section.section_id}: rating=${section.section_avg_rating}, difficulty=${section.section_avg_difficulty}, total_ratings=${section.section_total_ratings}`);
      });
    });

    return c.json({
      data: coursesWithSections,
      count: coursesWithSections.length,
      total_count: totalCount,
      has_more: hasMore,
      filters_applied: {
        status,
        min_available_seats,
        instruction_mode,
        min_section_avg_rating,
        min_section_avg_difficulty,
        min_section_total_ratings,
        min_section_avg_would_take_again,
      }
    });
  } catch (error) {
    console.error("Error in /api/query:", error);
    return c.json({ error: "Internal server error" }, 500);
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