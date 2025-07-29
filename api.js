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
      min_most_recent_gpa
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
        JOIN madgrades_course_grades ON courses.course_designation = madgrades_course_grades.course_name
        LEFT JOIN section_instructors si ON sections.section_id = si.section_id
        LEFT JOIN rmp_cleaned ON si.instructor_name = rmp_cleaned.full_name
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
             literature, level, madgrades_course_grades.cumulative_gpa, madgrades_course_grades.most_recent_gpa
      FROM courses 
      JOIN madgrades_course_grades ON courses.course_designation = madgrades_course_grades.course_name
      WHERE course_id IN (${coursePlaceholders})
      ORDER BY course_id
    `;

    const [coursesResults] = await pool.execute(coursesSql, courseIdList);

    // Get sections for these courses - apply same filters if any exist
    let sectionsSql, sectionsParams;

    if (sectionFilters.length > 0) {
      // Apply the same filters to sections query
      sectionsSql = `
        SELECT course_id, status, available_seats, waitlist_total, 
               capacity, enrolled, meeting_time, location, instruction_mode, is_asynchronous, sections.section_id,
               si.instructor_name, rmp_cleaned.avg_rating, rmp_cleaned.avg_difficulty, rmp_cleaned.num_ratings, rmp_cleaned.would_take_again_percent
        FROM sections 
        LEFT JOIN section_instructors si ON sections.section_id = si.section_id
        LEFT JOIN rmp_cleaned ON si.instructor_name = rmp_cleaned.full_name
        WHERE course_id IN (${coursePlaceholders}) AND ${sectionFilters.join(' AND ')}
        ORDER BY course_id, status DESC
      `;
      // Only pass section filter parameters (course filters already applied in first query)
      const sectionFilterParams = filterParams.slice(0, sectionFilters.length);
      sectionsParams = [...courseIdList, ...sectionFilterParams];
    } else {
      // No filters - get all sections
      sectionsSql = `
        SELECT course_id, status, available_seats, waitlist_total, 
               capacity, enrolled, meeting_time, location, instruction_mode, is_asynchronous, sections.section_id,
               si.instructor_name, rmp_cleaned.avg_rating, rmp_cleaned.avg_difficulty, rmp_cleaned.num_ratings, rmp_cleaned.would_take_again_percent
        FROM sections 
        LEFT JOIN section_instructors si ON sections.section_id = si.section_id
        LEFT JOIN rmp_cleaned ON si.instructor_name = rmp_cleaned.full_name
        WHERE course_id IN (${coursePlaceholders})
        ORDER BY course_id, status DESC
      `;
      sectionsParams = courseIdList;
    }

    const [sectionsResults] = await pool.execute(sectionsSql, sectionsParams);

    console.log("=== SECTIONS RESULTS DEBUG ===");
    console.log("Total sections returned:", sectionsResults.length);
    console.log("First few sections:", sectionsResults.slice(0, 3));
    console.log("================================");

    const sectionsByCourse = {};
    if (Array.isArray(sectionsResults)) {
      sectionsResults.forEach((row, index) => {
        console.log(`Processing row ${index}:`, {
          course_id: row.course_id,
          section_id: row.section_id,
          instructor_name: row.instructor_name,
          avg_rating: row.avg_rating,
          avg_difficulty: row.avg_difficulty,
          num_ratings: row.num_ratings,
          would_take_again_percent: row.would_take_again_percent
        });

        if (!sectionsByCourse[row.course_id]) {
          sectionsByCourse[row.course_id] = {};
          console.log(`Created new course entry for course_id: ${row.course_id}`);
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
            // Section-level averages will be calculated after all instructors are added
            section_avg_rating: null,
            section_avg_difficulty: null,
            section_total_ratings: 0,
            section_avg_would_take_again: null
          };
          console.log(`Created new section entry for section_id: ${row.section_id}`);
        }

        // Add instructor if it exists and isn't already added
        if (row.instructor_name) {
          const existingInstructor = sectionsByCourse[row.course_id][row.section_id].instructors
            .find(inst => inst.name === row.instructor_name);

          if (!existingInstructor) {
            const instructorData = {
              name: row.instructor_name,
              avg_rating: row.avg_rating,
              avg_difficulty: row.avg_difficulty,
              num_ratings: row.num_ratings,
              would_take_again_percent: row.would_take_again_percent
            };

            console.log(`Adding instructor to section ${row.section_id}:`, instructorData);
            sectionsByCourse[row.course_id][row.section_id].instructors.push(instructorData);
          } else {
            console.log(`Instructor ${row.instructor_name} already exists for section ${row.section_id}`);
          }
        } else {
          console.log(`No instructor name for section ${row.section_id}`);
        }
      });
    }

    console.log("=== SECTIONS BY COURSE BEFORE AVERAGING ===");
    Object.keys(sectionsByCourse).forEach(courseId => {
      console.log(`Course ${courseId}:`, Object.keys(sectionsByCourse[courseId]).length, "sections");
      Object.values(sectionsByCourse[courseId]).forEach(section => {
        console.log(`  Section ${section.section_id}: ${section.instructors.length} instructors`);
        section.instructors.forEach(inst => {
          console.log(`    - ${inst.name}: rating=${inst.avg_rating}, difficulty=${inst.avg_difficulty}, num_ratings=${inst.num_ratings}`);
        });
      });
    });

    // Calculate section-level averages after all instructors have been processed
    Object.values(sectionsByCourse).forEach(courseSections => {
      Object.values(courseSections).forEach(section => {
        console.log(`\n=== CALCULATING AVERAGES FOR SECTION ${section.section_id} ===`);
        console.log(`Total instructors: ${section.instructors.length}`);

        const instructorsWithRatings = section.instructors.filter(inst => {
          const hasRating = inst.avg_rating !== null && inst.avg_rating !== undefined;
          console.log(`Instructor ${inst.name}: has rating = ${hasRating} (rating: ${inst.avg_rating})`);
          return hasRating;
        });

        console.log(`Instructors with ratings: ${instructorsWithRatings.length}`);

        if (instructorsWithRatings.length > 0) {
          // Calculate weighted averages based on number of ratings
          let totalWeightedRating = 0;
          let totalWeightedDifficulty = 0;
          let totalWeightedWouldTakeAgain = 0;
          let totalRatings = 0;
          let validWouldTakeAgainCount = 0;

          instructorsWithRatings.forEach(instructor => {
            const weight = instructor.num_ratings || 1; // Use 1 as minimum weight if num_ratings is null
            console.log(`Processing instructor ${instructor.name}: weight=${weight}`);

            if (instructor.avg_rating !== null) {
              totalWeightedRating += instructor.avg_rating * weight;
              console.log(`  Rating contribution: ${instructor.avg_rating} * ${weight} = ${instructor.avg_rating * weight}`);
            }
            if (instructor.avg_difficulty !== null) {
              totalWeightedDifficulty += instructor.avg_difficulty * weight;
              console.log(`  Difficulty contribution: ${instructor.avg_difficulty} * ${weight} = ${instructor.avg_difficulty * weight}`);
            }
            if (instructor.would_take_again_percent !== null) {
              totalWeightedWouldTakeAgain += instructor.would_take_again_percent * weight;
              validWouldTakeAgainCount += weight;
              console.log(`  Would take again contribution: ${instructor.would_take_again_percent} * ${weight} = ${instructor.would_take_again_percent * weight}`);
            }

            totalRatings += weight;
          });

          console.log(`Totals: weighted_rating=${totalWeightedRating}, weighted_difficulty=${totalWeightedDifficulty}, total_ratings=${totalRatings}`);

          // Calculate section averages
          section.section_avg_rating = totalRatings > 0 ?
            Math.round((totalWeightedRating / totalRatings) * 100) / 100 : null;
          section.section_avg_difficulty = totalRatings > 0 ?
            Math.round((totalWeightedDifficulty / totalRatings) * 100) / 100 : null;
          section.section_total_ratings = totalRatings;
          section.section_avg_would_take_again = validWouldTakeAgainCount > 0 ?
            Math.round((totalWeightedWouldTakeAgain / validWouldTakeAgainCount) * 100) / 100 : null;

          console.log(`Final averages: rating=${section.section_avg_rating}, difficulty=${section.section_avg_difficulty}, total_ratings=${section.section_total_ratings}`);
        } else {
          console.log(`No instructors with ratings for section ${section.section_id}`);
        }
      });
    });

    // Convert sections object to array for each course
    Object.keys(sectionsByCourse).forEach(courseId => {
      sectionsByCourse[courseId] = Object.values(sectionsByCourse[courseId]);
    });

    console.log("=== FINAL SECTIONS BY COURSE ===");
    Object.keys(sectionsByCourse).forEach(courseId => {
      console.log(`Course ${courseId}: ${sectionsByCourse[courseId].length} sections (now as array)`);
    });

    // Combine courses with their sections (THIS WAS MISSING!)
    const coursesWithSections = coursesResults.map(course => {
      console.log(`Mapping course ${course.course_id} with sections:`, sectionsByCourse[course.course_id] ? sectionsByCourse[course.course_id].length : 0);
      return {
        ...course,
        sections: sectionsByCourse[course.course_id] || []
      };
    });

    console.log("=== FINAL COURSES WITH SECTIONS ===");
    console.log(`Total courses: ${coursesWithSections.length}`);
    coursesWithSections.forEach(course => {
      console.log(`Course ${course.course_id}: ${course.sections.length} sections`);
    });

    return c.json({
      data: coursesWithSections, // CHANGED: was returning sectionsByCourse directly
      count: coursesWithSections.length,
      filters_applied: {
        status,
        min_available_seats,
        instruction_mode
      }
    });
  } catch (error) {
    console.error("Error in /api/query:", error);
    return c.json({ error: "Internal server error" }, 500);
  }
});



app.get("/api/debug/rmp", async (c) => {
  const apiKey = c.req.header("x-api-key");
  if (!apiKey || apiKey !== Bun.env.GET_API_KEY) {
    return c.json({ error: "Unauthorized" }, 401);
  }

  try {
    // 1. Check sample instructor names from section_instructors
    const [instructorSample] = await pool.execute(`
      SELECT DISTINCT si.instructor_name 
      FROM section_instructors si 
      WHERE si.instructor_name IS NOT NULL 
      LIMIT 10
    `);

    // 2. Check sample names from rmp_cleaned
    const [rmpSample] = await pool.execute(`
      SELECT DISTINCT full_name, avg_rating, avg_difficulty, num_ratings 
      FROM rmp_cleaned 
      WHERE full_name IS NOT NULL 
      LIMIT 10
    `);

    // 3. Check for exact matches
    const [exactMatches] = await pool.execute(`
      SELECT si.instructor_name, rmp.full_name, rmp.avg_rating, rmp.avg_difficulty
      FROM section_instructors si
      INNER JOIN rmp_cleaned rmp ON si.instructor_name = rmp.full_name
      LIMIT 10
    `);

    // 4. Check specific instructors from your log
    const [specificCheck] = await pool.execute(`
      SELECT 
        'Stephanie Kann' as search_name,
        rmp.full_name,
        rmp.avg_rating,
        rmp.avg_difficulty,
        rmp.num_ratings,
        rmp.would_take_again_percent
      FROM rmp_cleaned rmp 
      WHERE rmp.full_name LIKE '%Kann%' OR rmp.full_name LIKE '%Stephanie%'
      
      UNION ALL
      
      SELECT 
        'Drew Graf' as search_name,
        rmp.full_name,
        rmp.avg_rating,
        rmp.avg_difficulty,
        rmp.num_ratings,
        rmp.would_take_again_percent
      FROM rmp_cleaned rmp 
      WHERE rmp.full_name LIKE '%Graf%' OR rmp.full_name LIKE '%Drew%'
    `);

    // 5. Check case sensitivity and whitespace issues
    const [caseCheck] = await pool.execute(`
      SELECT 
        si.instructor_name,
        LENGTH(si.instructor_name) as instructor_length,
        rmp.full_name,
        LENGTH(rmp.full_name) as rmp_length,
        si.instructor_name = rmp.full_name as exact_match,
        UPPER(TRIM(si.instructor_name)) = UPPER(TRIM(rmp.full_name)) as case_insensitive_match
      FROM section_instructors si
      LEFT JOIN rmp_cleaned rmp ON UPPER(TRIM(si.instructor_name)) = UPPER(TRIM(rmp.full_name))
      WHERE si.instructor_name IN ('Stephanie Kann', 'Drew Graf', 'Matthew Digman')
      LIMIT 20
    `);

    return c.json({
      instructor_sample: instructorSample,
      rmp_sample: rmpSample,
      exact_matches: exactMatches,
      specific_instructor_check: specificCheck,
      case_and_whitespace_check: caseCheck,
      debug_info: {
        total_instructors: instructorSample.length,
        total_rmp_records: rmpSample.length,
        exact_matches_found: exactMatches.length
      }
    });

  } catch (error) {
    console.error("Debug error:", error);
    return c.json({ error: error.message }, 500);
  }
});


app.get("/api/query/test", async (c) => {
  const apiKey = c.req.header("x-api-key");
  if (!apiKey || apiKey !== Bun.env.GET_API_KEY) {
    return c.json({ error: "Unauthorized" }, 401);
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