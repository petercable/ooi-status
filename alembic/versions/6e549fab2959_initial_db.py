"""Initial db

Revision ID: 6e549fab2959
Revises:
Create Date: 2016-04-25 06:51:55.120438

"""

# revision identifiers, used by Alembic.
revision = '6e549fab2959'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('expected_stream',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('method', sa.String(), nullable=False),
        sa.Column('expected_rate', sa.Float(), nullable=True),
        sa.Column('warn_interval', sa.Integer(), nullable=True),
        sa.Column('fail_interval', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'method')
    )
    op.create_table('deployed_stream',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('reference_designator', sa.String(), nullable=False),
        sa.Column('expected_stream_id', sa.Integer(), nullable=False),
        sa.Column('particle_count', sa.Integer(), nullable=False),
        sa.Column('last_seen', sa.DateTime(), nullable=False),
        sa.Column('collected', sa.DateTime(), nullable=False),
        sa.Column('expected_rate', sa.Float(), nullable=True),
        sa.Column('warn_interval', sa.Integer(), nullable=True),
        sa.Column('fail_interval', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['expected_stream_id'], ['expected_stream.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('reference_designator', 'expected_stream_id')
    )
    op.create_table('stream_count',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('stream_id', sa.Integer(), nullable=False),
        sa.Column('collected_time', sa.DateTime(), nullable=False),
        sa.Column('particle_count', sa.Integer(), nullable=True),
        sa.Column('seconds', sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(['stream_id'], ['deployed_stream.id'], ),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('stream_count')
    op.drop_table('deployed_stream')
    op.drop_table('expected_stream')
